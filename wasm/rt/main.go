package main

import (
	"fmt"
	"syscall/js"
)

func registerSlang() {
	var evalButton js.Func
	evalButton = js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		code := js.Global().Get("document").Call("getElementById", "code").Get("value").String()
		var res string
		func() {
			defer func() {
				if r := recover(); r != nil {
					res = fmt.Sprintln("[ERROR]", r)
				}
			}()
			res = Eval(code)
		}()
		js.Global().Get("document").Call("getElementById", "result").Set("value", res)
		return nil
	})
	var resetButton js.Func
	resetButton = js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		Reset()
		js.Global().Get("document").Call("getElementById", "result").Set("value", "Environment succesfully reset")
		return nil
	})

	js.Global().Get("document").Call("getElementById", "evalButton").Call("addEventListener", "click", evalButton)
	js.Global().Get("document").Call("getElementById", "resetButton").Call("addEventListener", "click", resetButton)
}

func main() {
	c := make(chan struct{}, 0)

	println("Go WebAssembly Initialized")
	registerSlang()

	<-c
}
