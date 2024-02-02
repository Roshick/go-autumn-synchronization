package cachedrepository

import "fmt"

type ErrRepositoryEntityNotFound struct {
	name string
}

func (e ErrRepositoryEntityNotFound) Error() string {
	return fmt.Sprintf("repository does not contain an entity '%s'", e.name)
}

func NewErrRepositoryEntityNotFound(name string) ErrRepositoryEntityNotFound {
	return ErrRepositoryEntityNotFound{name: name}
}

type ErrHashMismatch struct {
	providedHash string
	actualHash   string
}

func (e ErrHashMismatch) Error() string {
	return fmt.Sprintf("provided hash '%s' does not match current entity hash '%s'", e.providedHash, e.actualHash)
}

func NewErrHashMismatch(
	providedHash string,
	actualHash string,
) ErrHashMismatch {
	return ErrHashMismatch{
		providedHash: providedHash,
		actualHash:   actualHash,
	}
}
