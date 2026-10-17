package EventBus

import (
	"context"
	"fmt"
	"reflect"
	"unsafe"
)

type eventHandler struct {
	callBack      reflect.Value
	funcData      unsafe.Pointer // identifies the func value, see funcDataOf
	flagOnce      bool
	async         bool
	transactional bool
	sem           chan struct{} // lock for an event handler - useful for running async callbacks serially
}

var contextType = reflect.TypeFor[context.Context]()

// acquire takes the handler lock, giving up if ctx is done first.
func (handler *eventHandler) acquire(ctx context.Context) bool {
	select {
	case handler.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (handler *eventHandler) release() {
	<-handler.sem
}

// funcDataOf returns the data word of fn's interface value. For a func value this is
// the pointer to its closure, so it differs between closures of the same func literal,
// unlike reflect.Value.Pointer, which is the shared code pointer.
func funcDataOf(fn any) unsafe.Pointer {
	return (*[2]unsafe.Pointer)(unsafe.Pointer(&fn))[1]
}

// wantsContext reports whether the callback's first parameter is a context.Context.
func (handler *eventHandler) wantsContext() bool {
	funcType := handler.callBack.Type()
	return funcType.NumIn() > 0 && funcType.In(0) == contextType
}

// arguments builds the callback arguments. If the callback's first parameter
// is a context.Context, ctx is passed there and args fill the rest.
// A nil arg becomes the zero value of its parameter.
func (handler *eventHandler) arguments(ctx context.Context, topic string, args []any) []reflect.Value {
	funcType := handler.callBack.Type()
	passedArguments := make([]reflect.Value, 0, len(args)+1)
	if handler.wantsContext() {
		passedArguments = append(passedArguments, reflect.ValueOf(&ctx).Elem())
	}
	numIn := funcType.NumIn()
	variadic := funcType.IsVariadic()
	n := len(passedArguments) + len(args)
	if (!variadic && n != numIn) || (variadic && n < numIn-1) {
		panic(fmt.Sprintf("EventBus: topic %q: %d args for handler %s", topic, len(args), funcType))
	}
	for j, arg := range args {
		i := len(passedArguments)
		var paramType reflect.Type
		if variadic && i >= numIn-1 {
			paramType = funcType.In(numIn - 1).Elem()
		} else {
			paramType = funcType.In(i)
		}
		if arg == nil {
			passedArguments = append(passedArguments, reflect.Zero(paramType))
			continue
		}
		value := reflect.ValueOf(arg)
		if !value.Type().AssignableTo(paramType) {
			panic(fmt.Sprintf("EventBus: topic %q: arg %d is %s, handler %s wants %s", topic, j, value.Type(), funcType, paramType))
		}
		passedArguments = append(passedArguments, value)
	}
	return passedArguments
}
