package cachedrepository

import "golang.org/x/net/context"

type noProcessor[BaseEntity any] struct {
}

func NewNoProcessor[BaseEntity any]() Processor[BaseEntity, BaseEntity] {
	return &noProcessor[BaseEntity]{}
}

func (p *noProcessor[BaseEntity]) ProcessEntity(
	_ context.Context,
	entity BaseEntity,
) (BaseEntity, error) {
	return entity, nil
}

func (p *noProcessor[BaseEntity]) InverseProcessEntity(
	_ context.Context,
	entity BaseEntity,
) (BaseEntity, error) {
	return entity, nil
}
