package main

import (
	"testing"
	"context"
	"time"
	"os"
)

func TestConnection(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	dataClient := createClient(ctx, db)

	w := os.Stdout
	err, id := simpleSelect(ctx, w, dataClient)
	if err != nil {
		t.Errorf(" failed with %v", err)
	} else if id != 1 {
		t.Errorf("expected 1 found %d", id)
	}
}

func BenchmarkEmptyRWTransaction(b *testing.B) {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	dataClient := createClient(ctx, db)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := emptyRWTransaction(ctx, dataClient)
		if err != nil {
			b.Errorf(" failed with %v", err)
			b.Fail()
		}
	}
}

func BenchmarkCallSetup(b *testing.B) {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	dataClient := createClient(ctx, db)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := callSetup(ctx, dataClient)
			if err != nil {
				b.Errorf(" failed with %v", err)
				b.Fail()
			}
		}
	})
}
