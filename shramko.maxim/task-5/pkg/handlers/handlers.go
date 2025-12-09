package handlers

import (
	"context"
	"errors"
	"strings"
	"sync"
)

var ErrDecorationNotAllowed = errors.New("can't be decorated")

const (
	noDecorationMarker  = "no decorator"
	decorationMark      = "decorated: "
	noMultiplexingLabel = "no multiplexer"
)

func PrefixDecoratorFunc(ctx context.Context, input chan string, output chan string) error {
	defer close(output)

	for {
		select {
		case <-ctx.Done():
			return nil
		case data, valid := <-input:
			if !valid {
				return nil
			}

			if strings.Contains(data, noDecorationMarker) {
				return ErrDecorationNotAllowed
			}

			if !strings.HasPrefix(data, decorationMark) {
				data = decorationMark + data
			}

			select {
			case <-ctx.Done():
				return nil
			case output <- data:
			}
		}
	}
}

func SeparatorFunc(ctx context.Context, input chan string, outputs []chan string) error {
	defer func() {
		for _, out := range outputs {
			close(out)
		}
	}()

	outputCount := len(outputs)
	pos := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case data, valid := <-input:
			if !valid {
				return nil
			}

			if outputCount == 0 {
				continue
			}

			target := outputs[pos%outputCount]
			pos++

			select {
			case <-ctx.Done():
				return nil
			case target <- data:
			}
		}
	}
}

func MultiplexerFunc(ctx context.Context, inputs []chan string, output chan string) error {
	defer close(output)

	var wg sync.WaitGroup
	done := make(chan struct{})

	for _, in := range inputs {
		wg.Add(1)
		go func(src chan string) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				case data, valid := <-src:
					if !valid {
						return
					}

					if strings.Contains(data, noMultiplexingLabel) {
						continue
					}

					select {
					case <-done:
						return
					case <-ctx.Done():
						return
					case output <- data:
					}
				}
			}
		}(in)
	}

	wg.Wait()
	close(done)
	return nil
}
