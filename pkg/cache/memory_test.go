package aucache

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"testing"
	"time"
)

type demoEntity struct {
	Value1 string `json:"v1"`
	Value2 *string
	Value3 *map[string]string
}

func TestEmptyMemoryCache(t *testing.T) {
	ctx := context.TODO()
	cut := NewMemoryCache[demoEntity]()

	entries, err := cut.Entries(ctx)
	require.Nil(t, err)
	require.NotNil(t, entries)
	require.Empty(t, entries)

	keys, err := cut.Keys(ctx)
	require.Nil(t, err)
	require.NotNil(t, keys)
	require.Empty(t, keys)

	values, err := cut.Values(ctx)
	require.Nil(t, err)
	require.NotNil(t, values)
	require.Empty(t, values)

	got, err := cut.Get(ctx, "non-existing-key")
	require.Nil(t, err)
	require.Nil(t, got)
}

func TestPopulatedMemoryCache(t *testing.T) {
	ctx := context.TODO()
	cut := NewMemoryCache[demoEntity]()

	e1 := demoEntity{
		Value1: "value-for-v1",
		Value2: p("value-for-v2"),
		Value3: p(map[string]string{"mapkey1": "mapvalue1", "mapkey2": "mapvalue2"}),
	}
	err := cut.Set(ctx, "key1", e1, 6*time.Hour)
	require.Nil(t, err)

	e2 := demoEntity{
		Value1: "e2-value-for-v1",
		Value2: p("e2-value-for-v2"),
		Value3: p(map[string]string{"mapkey1": "e2-mapvalue1", "mapkey2": "e2-mapvalue2"}),
	}
	err = cut.Set(ctx, "akey2", e2, 6*time.Hour)
	require.Nil(t, err)

	entries, err := cut.Entries(ctx)
	require.Nil(t, err)
	require.NotNil(t, entries)
	require.EqualValues(t, map[string]demoEntity{
		"key1":  e1,
		"akey2": e2,
	}, entries)

	keys, err := cut.Keys(ctx)
	require.Nil(t, err)
	require.NotNil(t, keys)
	require.Len(t, keys, 2)
	require.Contains(t, keys, "akey2")
	require.Contains(t, keys, "key1")

	values, err := cut.Values(ctx)
	require.Nil(t, err)
	require.NotNil(t, values)
	require.Len(t, values, 2)
	require.Contains(t, values, e1)
	require.Contains(t, values, e2)

	got, err := cut.Get(ctx, "akey2")
	require.Nil(t, err)
	require.NotNil(t, got)
	require.EqualValues(t, e2, *got)

	err = cut.Remove(ctx, "akey2")
	require.Nil(t, err)

	keys, err = cut.Keys(ctx)
	require.Nil(t, err)
	require.NotNil(t, keys)
	require.Len(t, keys, 1)
	require.Contains(t, keys, "key1")
}

func TestMemoryCacheDeepcopy(t *testing.T) {
	ctx := context.TODO()
	cut := NewMemoryCache[demoEntity]()

	e1 := demoEntity{
		Value1: "value-for-v1",
		Value2: p("value-for-v2"),
		Value3: p(map[string]string{"mapkey1": "mapvalue1", "mapkey2": "mapvalue2"}),
	}
	err := cut.Set(ctx, "key1", e1, 6*time.Hour)
	require.Nil(t, err)

	e1.Value2 = nil
	(*e1.Value3)["other"] = "stuff"
	err = cut.Set(ctx, "key2", e1, 6*time.Hour)
	require.Nil(t, err)

	e1orig := demoEntity{
		Value1: "value-for-v1",
		Value2: p("value-for-v2"),
		Value3: p(map[string]string{"mapkey1": "mapvalue1", "mapkey2": "mapvalue2"}),
	}
	got, err := cut.Get(ctx, "key1")
	require.Nil(t, err)
	require.NotNil(t, got)
	require.EqualValues(t, e1orig, *got)

	got, err = cut.Get(ctx, "key2")
	require.Nil(t, err)
	require.NotNil(t, got)
	require.EqualValues(t, e1, *got)
}

func p[E any](v E) *E {
	return &v
}
