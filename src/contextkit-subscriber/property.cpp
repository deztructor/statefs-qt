#include "property.hpp"
#include <statefs/qt/util.hpp>

#include <cor/mt.hpp>
#include <cor/util.hpp>

#include <contextproperty.h>
#include <QDebug>
#include <QTimer>
#include <QSocketNotifier>
#include <QMutex>
#include <memory>
#include <poll.h>

namespace {

// to wrap code to be executed from the code which does not support
// exception handling
template <typename F>
void execute_nothrow(F fn, char const *msg)
{
    try{
        fn();
    } catch (std::future_error const &e) {
        // skip, nothing can be done, maybe Qt has not delivered
        // QEvent to the target, maybe something else is happened. Do
        // not trace it because it can happen frequently in cases when
        // component is frequently created/deleted
    } catch (std::exception const &e) {
        qWarning() << "Ignoring exception: " << e.what() << ". " << msg;
    // } catch (...) {
    //     // Those exceptions should not be catched, let's abort. Maybe
    //     // this is something bad
    //     qWarning() << "Unknown exception, ignoring. " << msg;
    }
}
}

namespace ckit
{

class Event : public QEvent
{
public:
    enum Type {
        Subscribe = QEvent::User,
        Unsubscribe,
        Subscribed
    };

    virtual ~Event();

protected:
    Event(Type t);
private:
    Event();

};

QMutex PropertyMonitor::actorGuard_;
PropertyMonitor::monitor_ptr PropertyMonitor::instance_;

PropertyMonitor::monitor_ptr PropertyMonitor::instance()
{
    monitor_ptr p = instance_;
    if (p)
        return p;

    QMutexLocker lock(&actorGuard_);

    if (instance_)
        return instance_;

    p.reset(new monitor_type([]() {
                return new ckit::PropertyMonitor();
            }));
    new ExitMonitor(p);
    p->startSync();
    instance_ = p;

    return instance_;
}


Event::Event(Event::Type t)
    : QEvent(static_cast<QEvent::Type>(t))
{}

Event::~Event() {}

class SubscribeRequest : public Event
{
public:
    SubscribeRequest(ContextPropertyPrivate const *tgt
                    , QString const&key
                    , std::promise<QVariant> &&res)
        : Event(Event::Subscribe)
        , tgt_(tgt)
        , key_(key)
        , value_(std::move(res))
    {}
    virtual ~SubscribeRequest() {}

    ContextPropertyPrivate const *tgt_;
    QString key_;
    std::promise<QVariant> value_;

private:
    SubscribeRequest(SubscribeRequest const&);
};


class UnsubscribeRequest : public Event
{
public:
    UnsubscribeRequest(ContextPropertyPrivate const *tgt
                    , QString const &key
                    , std::promise<void> &&done)
        : Event(Event::Unsubscribe)
        , tgt_(tgt)
        , key_(key)
        , done_(std::move(done))
    {}
    virtual ~UnsubscribeRequest() {}

    UnsubscribeRequest(SubscribeRequest const&) = delete;

    ContextPropertyPrivate const *tgt_;
    QString key_;
    std::promise<void> done_;
};

bool PropertyMonitor::event(QEvent *e)
{
    if (e->type() < QEvent::User)
        return QObject::event(e);

    auto res = true;
    auto fn = [this, e, &res]() {
        auto t = static_cast<Event::Type>(e->type());
        switch (t) {
        case Event::Subscribe: {
            auto p = dynamic_cast<SubscribeRequest*>(e);
            if (p)
                subscribe(p);
            else
                qWarning() << "PropertyMonitor: !SubscribeRequest";
            break;
        }
        case Event::Unsubscribe: {
            auto p = dynamic_cast<UnsubscribeRequest*>(e);
            if (p)
                unsubscribe(p);
            else
                qWarning() << "PropertyMonitor: !UnsubscribeRequest";
            break;
        }
        default:
            qWarning() << "Unknown user event";
            res = QObject::event(e);
        }
    };
    execute_nothrow(fn, __PRETTY_FUNCTION__);
    return res;
}

void PropertyMonitor::subscribe(SubscribeRequest *req)
{
    auto tgt = req->tgt_;
    auto key = req->key_;
    Property *handler;
    QVariant retval;

    if (!tgt) {
        qWarning() << "Logic issue: subscription target is null";
        return;
    }
    auto notify_fn = [req, &retval, tgt]() {
        req->value_.set_value(retval);
        QMetaObject::invokeMethod
        (const_cast<ContextPropertyPrivate*>(tgt), "onChanged"
         , Qt::QueuedConnection, Q_ARG(QVariant, retval));
    };
    auto notify_on_exit = cor::on_scope_exit([notify_fn]() {
            execute_nothrow(notify_fn, __PRETTY_FUNCTION__);
        });

    if (key.isEmpty()) {
        qWarning() << "Empty contextkit key";
        return;
    }

    auto it = targets_.find(key);
    if (it == targets_.end()) {
        it = targets_.insert(key, QSet<ContextPropertyPrivate const*>());
        it->insert(tgt);
        handler = add(key);
    } else {
        if (it->contains(tgt)) {
            return;
        }
        it->insert(tgt);
        handler = properties_[key];
    }

    connect(handler, SIGNAL(changed(QVariant))
            , tgt, SLOT(onChanged(QVariant)));

    retval = handler->subscribe();
}

void PropertyMonitor::unsubscribe(UnsubscribeRequest *req)
{
    auto on_exit = cor::on_scope_exit([req]() {
            execute_nothrow([req]() {
                    req->done_.set_value();
                }, __PRETTY_FUNCTION__);
        });

    auto tgt = req->tgt_;
    auto key = req->key_;

    auto ptargets = targets_.find(key);
    if (ptargets == targets_.end())
        return;

    if (!ptargets->remove(tgt))
        return;

    auto phandlers = properties_.find(key);
    if (phandlers == properties_.end())
        return;

    auto handler = phandlers.value();

    if (ptargets->isEmpty()) {
        // last subscriber is gone
        targets_.erase(ptargets);
        properties_.erase(phandlers);
        handler->deleteLater();
    }
}

Property* PropertyMonitor::add(const QString &key)
{
    auto it = properties_.insert(key, new Property(key, this));
    return it.value();
}

Property::Property(const QString &key, QObject *parent)
    : QObject(parent)
    , key_(key)
    , user_file_(statefs::qt::getPath(key))
    , sys_file_(statefs::qt::getSystemPath(key))
    , file_(&user_file_)
    , notifier_(nullptr)
    , reopen_interval_(100)
    , reopen_timer_(new QTimer(this))
    , is_subscribed_(false)
{
    reopen_timer_->setSingleShot(true);
    connect(reopen_timer_, SIGNAL(timeout()), this, SLOT(trySubscribe()));
}

Property::~Property()
{
    unsubscribe();
}

void Property::trySubscribe()
{
    static const int max_interval_ = 1000 * 60 * 3;
    static const int fast_interval_ = 1000 * 3;
    static const int slow_interval_ = 1000 * 30;
    if (tryOpen()) {
        reopen_interval_ = 500;
        subscribe_();
        return;
    }

    if (reopen_interval_ < fast_interval_) {
        reopen_interval_ *= 2;
    } else if (reopen_interval_ < slow_interval_) {
        reopen_interval_ += fast_interval_;
    } else if (reopen_interval_ < max_interval_) {
        reopen_interval_ += slow_interval_;
    } else {
        reopen_interval_ = 1000 * 60 * 3;
    }
    reopen_timer_->start(reopen_interval_);
}

void Property::resubscribe()
{
    if (is_subscribed_) {
        unsubscribe();
        subscribe_();
    }
}

// 1MB?
static const size_t max_statefs_file_size = 1024 * 1024;

static int read(QIODevice &src, QByteArray &dst
                , size_t size, size_t offset = 0)
{
    if ((size_t)dst.size() < offset + size) {
        qWarning() << "Logical error: wrong dst QByteArray size";
        return 0;
    }
    return src.read(dst.data() + offset, size);
}

bool Property::update()
{
    static const size_t cap = 31;
    bool is_updated = false;

    if (!tryOpen()) {
        qWarning() << "Can't open " << file_->fileName();
        cache_ = statefs::qt::valueDefault(cache_);
        resubscribe();
        return is_updated;
    }

    // WORKAROUND: file is just opened and closed before reading from
    // real source to make vfs (?) reread file data to cache
    QFile touchFile(file_->fileName());
    touchFile.open(QIODevice::ReadOnly | QIODevice::Unbuffered);

    file_->seek(0);
    auto size = file_->size();
    // statefs file size can change, so need to read more and check:
    // if amount of data read > size, continue to read. Also readAll()
    // is not suitable for the same reason
    auto to_read = size + cap;
    if (buffer_.size() < to_read + 1)
        buffer_.resize(to_read + 1 /* for \0 */);

    int rc = read(*file_, buffer_, to_read);
    // read all data
    if (rc > size) {
        int bytes_read = 0;
        while (rc > 0) {
            bytes_read += rc;
            if ((size_t)bytes_read > max_statefs_file_size) {
                qWarning() << "File size for " << file_->fileName()
                           << "reached max " << max_statefs_file_size;
                break;
            }
            // maybe there is more data to read
            buffer_.resize(buffer_.size() + bytes_read + 1);
            rc = read(*file_, buffer_, bytes_read, bytes_read);
        }
        rc = bytes_read;
    }
    touchFile.close();
    if (rc >= 0) {
        buffer_[rc] = '\0';
        auto s = QString(buffer_);
        if (s.size()) {
            cache_ = statefs::qt::valueDecode(s);
        } else {
            if (cache_.isNull()) {
                cache_ = s;
            } else {
                cache_ = statefs::qt::valueDefault(cache_);
            }
        }
        is_updated = true;
    } else {
        qWarning() << "Error accessing? " << rc << "..." << file_->fileName();
        resubscribe();
    }
    return is_updated;
}


void Property::handleActivated(int)
{
    if (update())
        emit changed(cache_);
}

Property::OpenResult Property::tryOpen(QFile &f)
{
    if (f.isOpen())
        return Opened;

    if (!f.exists())
        return DoesntExists;

    f.open(QIODevice::ReadOnly | QIODevice::Unbuffered);
    if (!f.isOpen())
        return CantOpen;

    return Opened;
}

bool Property::tryOpen()
{
    OpenResult res0, res1;
    res0 = tryOpen(*file_);
    if (res0 == Opened)
        return true;

    file_ = (file_ == &user_file_) ? &sys_file_ : &user_file_;

    res1 = tryOpen(*file_);
    if (res1 == Opened)
        return true;

    auto info0 = (res0 == DoesntExists ? "no file" : "can't open");
    auto info1 = (res1 == DoesntExists ? "no file" : "can't open");
    QString f0, f1;
    if (file_ == &user_file_) {
        f0 = "Sys";
        f1 = "User";
    } else {
        f0 = "User";
        f1 = "Sys";
    }
    qWarning() << "Error accessing property " << key_ << ": "
               << f0 << ": " << info0 << ", " << f1 << ": " << info1;
    file_ = &user_file_;
    return false;
}

QVariant Property::subscribe()
{
    return (!is_subscribed_ ? subscribe_() : cache_);
}

QVariant Property::subscribe_()
{
    if (!tryOpen()) {
        reopen_timer_->start(reopen_interval_);
        return QVariant();
    }
    is_subscribed_ = true;

    notifier_.reset(new QSocketNotifier
                    (file_->handle(), QSocketNotifier::Read));
    connect(notifier_.data(), SIGNAL(activated(int))
            , this, SLOT(handleActivated(int)));

    if (update())
        emit changed(cache_);

    return cache_;
}

void Property::unsubscribe()
{
    if (!is_subscribed_)
        return;

    is_subscribed_ = false;

    notifier_.reset();

    if (file_->isOpen())
        file_->close();
}

}


ContextPropertyPrivate::ContextPropertyPrivate(const QString &key, QObject *parent)
    : QObject(parent)
    , key_(key)
    , state_(Initial)
    , is_cached_(false)
{
}

ContextPropertyPrivate::~ContextPropertyPrivate()
{
    unsubscribe();
}

QString ContextPropertyPrivate::key() const
{
    return key_;
}

QVariant ContextPropertyPrivate::value(const QVariant &defVal) const
{
    return is_cached_ ? cache_ : defVal;
}

QVariant ContextPropertyPrivate::value() const
{
    return value(QVariant());
}

ckit::PropertyMonitor::monitor_ptr ContextPropertyPrivate::actor()
{
    return ckit::PropertyMonitor::instance();
}

void ContextPropertyPrivate::onChanged(QVariant v) const
{
    if (state_ == Subscribing)
        state_ = Subscribed;

    if (update(v))
        emit valueChanged();
}

bool ContextPropertyPrivate::update(QVariant const &v) const
{
    bool res = true;
    if (!is_cached_) {
        cache_ = v;
        is_cached_ = true;
    } else if (v != cache_) {
        cache_ = v;
    } else {
        res = false;
    }
    return res;
}


const ContextPropertyInfo* ContextPropertyPrivate::info() const
{
    return nullptr; // TODO
}

void ContextPropertyPrivate::subscribe() const
{
    auto fn = [this]() {
        if (state_ == Subscribing || state_ == Subscribed)
            return;

        // unsubscription is asynchronous, so wait for it to be finished
        // if resubcribing
        if (state_ == Unsubscribing)
            on_unsubscribed_.wait_for(std::chrono::milliseconds(20000));

        state_ = Subscribing;
        std::promise<QVariant> res;
        on_subscribed_ = res.get_future();
        auto ev = new ckit::SubscribeRequest(this, key_, std::move(res));
        actor()->postEvent(ev);
    };
    execute_nothrow(fn, __PRETTY_FUNCTION__);
}

void ContextPropertyPrivate::unsubscribe() const
{
    auto fn = [this]() {
        if (state_ == Unsubscribing)
            return;

        std::promise<void> res;
        on_unsubscribed_ = res.get_future();
        auto ev = new ckit::UnsubscribeRequest(this, key_, std::move(res));
        actor()->postEvent(ev);
        state_ = Unsubscribing;
    };
    execute_nothrow(fn, __PRETTY_FUNCTION__);
}


void ContextPropertyPrivate::waitForSubscription() const
{
    auto fn = [this]() {
        if (state_ != Subscribing)
            return;

        auto status = cor::wait_for(on_subscribed_, std::chrono::milliseconds(2000));
        if (status == std::future_status::ready) {
            update(on_subscribed_.get());
        }
        state_ = Subscribed;
    };
    execute_nothrow(fn, __PRETTY_FUNCTION__);
}

void ContextPropertyPrivate::waitForSubscription(bool block) const
{
    auto fn = [this, block]() {
        if (state_ != Subscribing)
            return;

        if (block)
            return waitForSubscription();

        while (state_ != Subscribed)
            QCoreApplication::processEvents();

        state_ = Subscribed;
    };
    execute_nothrow(fn, __PRETTY_FUNCTION__);
}

void ContextPropertyPrivate::ignoreCommander()
{
}

void ContextPropertyPrivate::setTypeCheck(bool typeCheck)
{
}


ContextProperty::ContextProperty(const QString &key, QObject *parent)
    : QObject(parent)
    , priv(new ContextPropertyPrivate(key, this))
{
    connect(priv, SIGNAL(valueChanged()), this, SIGNAL(valueChanged()));
    priv->subscribe();
}

ContextProperty::~ContextProperty()
{
    disconnect(priv, SIGNAL(valueChanged()), this, SIGNAL(valueChanged()));
}

QString ContextProperty::key() const
{
    return priv->key();
}

QVariant ContextProperty::value(const QVariant &def) const
{
    return priv->value(def);
}

QVariant ContextProperty::value() const
{
    return priv->value();
}

const ContextPropertyInfo* ContextProperty::info() const
{
    return priv->info();
}

void ContextProperty::subscribe () const
{
    return priv->subscribe();
}

void ContextProperty::unsubscribe () const
{
    return priv->unsubscribe();
}

void ContextProperty::waitForSubscription() const
{
    return priv->waitForSubscription();
}

void ContextProperty::waitForSubscription(bool block) const
{
    return priv->waitForSubscription(block);
}

void ContextProperty::ignoreCommander()
{
}

void ContextProperty::setTypeCheck(bool typeCheck)
{
}

#if QT_VERSION < 0x050000
void ContextProperty::onValueChanged() { }
#endif
