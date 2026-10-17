package EventBus

import "context"

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(topic string, fn any) error
	SubscribeAsync(topic string, fn any, transactional bool) error
	SubscribeOnce(topic string, fn any) error
	SubscribeOnceAsync(topic string, fn any) error
	Unsubscribe(topic string, handler any) error
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(topic string, args ...any)
	PublishContext(ctx context.Context, topic string, args ...any) error
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
	WaitAsyncContext(ctx context.Context) error
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}
