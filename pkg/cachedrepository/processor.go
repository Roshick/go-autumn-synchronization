package cachedrepository

import "golang.org/x/net/context"

type noProcessor[B any] struct {
}

func NewNoProcessor[B any]() Processor[B, B] {
	return &noProcessor[B]{}
}

func (p *noProcessor[B]) ProcessEntity(
	_ context.Context,
	entity B,
) (B, error) {
	return entity, nil
}

func (p *noProcessor[B]) InverseProcessEntity(
	_ context.Context,
	entity B,
) (B, error) {
	return entity, nil
}
