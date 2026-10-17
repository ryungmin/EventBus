package EventBus

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func newTestHandler(fn any) *eventHandler {
	return &eventHandler{callBack: reflect.ValueOf(fn), funcData: funcDataOf(fn), sem: make(chan struct{}, 1)}
}

// call builds the arguments for fn and calls it.
func call(t *testing.T, fn any, ctx context.Context, args ...any) {
	t.Helper()
	handler := newTestHandler(fn)
	handler.callBack.Call(handler.arguments(ctx, "topic", args))
}

func TestArgumentsInjectsContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	var got context.Context
	var gotS string
	call(t, func(c context.Context, s string) { got, gotS = c, s }, ctx, "x")
	if got != ctx || gotS != "x" {
		t.Errorf("got (%v, %q), want (ctx, \"x\")", got, gotS)
	}
}

func TestArgumentsContextOnlyAsFirstParam(t *testing.T) {
	// a context.Context that is not the first parameter is an ordinary arg
	ctx := context.Background()
	var got context.Context
	call(t, func(s string, c context.Context) { got = c }, context.TODO(), "x", ctx)
	if got != ctx {
		t.Errorf("got %v, want the ctx passed in args", got)
	}
}

type notContext struct{ context.Context }

func TestArgumentsContextExactType(t *testing.T) {
	// a first parameter that only implements context.Context gets no injection
	want := notContext{context.Background()}
	var got notContext
	call(t, func(c notContext) { got = c }, context.TODO(), want)
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestArgumentsNilBecomesZeroValue(t *testing.T) {
	var (
		gotInt   = -1
		gotPtr   = new(int)
		gotErr   = errors.New("set")
		gotSlice = []int{1}
		gotMap   = map[string]int{"a": 1}
	)
	call(t, func(i int, p *int, e error, s []int, m map[string]int) {
		gotInt, gotPtr, gotErr, gotSlice, gotMap = i, p, e, s, m
	}, context.Background(), nil, nil, nil, nil, nil)
	if gotInt != 0 || gotPtr != nil || gotErr != nil || gotSlice != nil || gotMap != nil {
		t.Errorf("got (%v, %v, %v, %v, %v), want zero values", gotInt, gotPtr, gotErr, gotSlice, gotMap)
	}
}

func TestArgumentsAssignableToInterface(t *testing.T) {
	wantErr := errors.New("boom")
	var gotErr error
	var gotAny any
	call(t, func(e error, a any) { gotErr, gotAny = e, a }, context.Background(), wantErr, 42)
	if gotErr != wantErr || gotAny != 42 {
		t.Errorf("got (%v, %v), want (%v, 42)", gotErr, gotAny, wantErr)
	}
}

func TestArgumentsVariadic(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want []int
	}{
		{"no extra args", []any{"p"}, nil},
		{"one extra arg", []any{"p", 1}, []int{1}},
		{"many extra args", []any{"p", 1, 2, 3}, []int{1, 2, 3}},
		{"nil extra arg", []any{"p", nil}, []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int
			call(t, func(ctx context.Context, prefix string, rest ...int) { got = rest }, context.Background(), tt.args...)
			// reflect passes an empty slice, not nil, when there are no extra args
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArgumentsMismatchPanics(t *testing.T) {
	tests := []struct {
		name    string
		fn      any
		args    []any
		message string
	}{
		{"too few", func(a, b int) {}, []any{1}, `topic "topic": 1 args for handler func(int, int)`},
		{"too many", func(a int) {}, []any{1, 2}, `topic "topic": 2 args for handler func(int)`},
		{"none for one", func(a int) {}, nil, `topic "topic": 0 args for handler func(int)`},
		{"wrong type", func(a int) {}, []any{"x"}, `topic "topic": arg 0 is string, handler func(int) wants int`},
		{"ctx not counted in args", func(ctx context.Context, a int) {}, nil, "0 args"},
		{"ctx passed by hand", func(ctx context.Context, a int) {}, []any{context.Background(), 1}, "2 args"},
		{"variadic too few", func(a int, rest ...int) {}, nil, "0 args"},
		{"variadic wrong type", func(rest ...int) {}, []any{1, "x"}, "arg 1 is string"},
		{"not assignable to interface", func(e error) {}, []any{1}, "arg 0 is int"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				message, _ := r.(string)
				if !strings.Contains(message, tt.message) {
					t.Errorf("got panic %v, want it to contain %q", r, tt.message)
				}
			}()
			newTestHandler(tt.fn).arguments(context.Background(), "topic", tt.args)
		})
	}
}

func TestWantsContext(t *testing.T) {
	tests := []struct {
		fn   any
		want bool
	}{
		{func() {}, false},
		{func(int) {}, false},
		{func(context.Context) {}, true},
		{func(context.Context, int) {}, true},
		{func(int, context.Context) {}, false},
		{func(...context.Context) {}, false},
	}
	for _, tt := range tests {
		if got := newTestHandler(tt.fn).wantsContext(); got != tt.want {
			t.Errorf("%T: got %v, want %v", tt.fn, got, tt.want)
		}
	}
}

func topLevelHandler() {}

func TestFuncDataOf(t *testing.T) {
	newClosure := func(n int) func() int { return func() int { return n } }
	first, second := newClosure(1), newClosure(2)
	again := first
	if funcDataOf(first) != funcDataOf(again) {
		t.Error("same closure gave different data")
	}
	if funcDataOf(first) == funcDataOf(second) {
		t.Error("different closures of one func literal gave the same data")
	}
	if top := topLevelHandler; funcDataOf(top) != funcDataOf(topLevelHandler) {
		t.Error("top-level func gave different data")
	}
	if funcDataOf(first) == nil {
		t.Error("got nil data for a closure")
	}
}

func TestAcquireRelease(t *testing.T) {
	handler := newTestHandler(func() {})
	if !handler.acquire(context.Background()) {
		t.Fatal("first acquire failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if handler.acquire(ctx) {
		t.Error("acquire of a held lock succeeded with a canceled ctx")
	}
	handler.release()
	if !handler.acquire(context.Background()) {
		t.Error("acquire after release failed")
	}
}
