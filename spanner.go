package main

import (
	"io"
	"log"

	"cloud.google.com/go/spanner"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"math/rand"
	"time"
)

var db = "projects/redknee-cloud-spanner/instances/myinstance/databases/mydb"

func simpleSelect(ctx context.Context, w io.Writer, client *spanner.Client) (error, int64) {
	stmt := spanner.Statement{SQL: `SELECT 1`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil, 0
		}
		if err != nil {
			return err, 0
		}
		var id int64
		if err := row.Columns(&id); err != nil {
			return err, 0
		}
		return err, id
	}
}

func emptyRWTransaction(ctx context.Context, client *spanner.Client) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return nil
	})
	return err
}

func createClient(ctx context.Context, db string) (*spanner.Client) {
	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}

func callSetup(ctx context.Context, client *spanner.Client) error {
	lCustomerId := 491733000000 + rand.Int63n(100000000);
	stmt := spanner.NewStatement( `SELECT mp FROM cao_ldm_00_acc WHERE pk=@pk AND valid_from<=@valid_from ORDER BY valid_from DESC`)
	stmt.Params["pk"] = "169 " + string(lCustomerId) + " 0 "
	stmt.Params["valid_from"] = 100500

	iter := client.Single().Query(ctx, stmt)
	var mp int64
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		if err := row.Columns(&mp); err != nil {
			return err
		}
	}
	cols := []string{"mp", "cc", "fu_01", "fu_02", "fu_03", "fu_04", "su_01", "su_02", "su_03", "su_04"}

	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "cao_ldm_00_ent", spanner.Key{mp}, cols)
		if err != nil {
			return err
		}

		var cc int64
		if err := row.ColumnByName("cc", &cc); err != nil {
			return err
		}

		time.Sleep(20 * time.Millisecond)
		txn.BufferWrite([]*spanner.Mutation{
			spanner.Update("cao_ldm_00_ent", []string{"mp", "cc"}, []interface{}{mp, cc }),

		})
		return nil
	})

	if err != nil {
		return err
	} else {
		return nil
	}
}

func prefetch(ctx context.Context, client *spanner.Client) error {
	lCustomerId := 491733000000 + rand.Int63n(100000000);
	stmt := spanner.NewStatement( `SELECT mp FROM cao_ldm_00_acc WHERE pk=@pk AND valid_from<=@valid_from ORDER BY valid_from DESC`)
	stmt.Params["pk"] = "169 " + string(lCustomerId) + " 0 "
	stmt.Params["valid_from"] = 100500

	iter := client.Single().Query(ctx, stmt)
	var mp int64
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		if err := row.Columns(&mp); err != nil {
			return err
		}
	}
	return nil
}


func main()  {
	
}
