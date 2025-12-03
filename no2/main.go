package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "sync"

    "github.com/qdrant/go-client/qdrant"
    "google.golang.org/grpc"
)

func main() {
    ctx := context.Background()

    conn, err := grpc.Dial("localhost:6334", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }

    points := qdrant.NewPointsClient(conn)
    collections := qdrant.NewCollectionsClient(conn)

    _, _ = collections.Delete(ctx, &qdrant.DeleteCollection{CollectionName: "articles"})

    _, err = collections.Create(ctx, &qdrant.CreateCollection{
        CollectionName: "articles",
        VectorsConfig: &qdrant.VectorsConfig{
            Config: &qdrant.VectorsConfig_Params{
                Params: &qdrant.VectorParams{
                    Size:     4,
                    Distance: qdrant.Distance_Cosine,
                },
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Collection 'articles' created.")

    pts := []*qdrant.PointStruct{
        NewPoint(1, []float32{0.1, 0.2, 0.3, 0.4}, "Go Tips"),
        NewPoint(2, []float32{0.9, 0.1, 0.1, 0.3}, "Javascript Tips"),
        NewPoint(3, []float32{0.2, 0.4, 0.6, 0.8}, "Typescript Tips"),
    }

    _, err = points.Upsert(ctx, &qdrant.UpsertPoints{
        CollectionName: "articles",
        Points:         pts,
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("3 Artikel inserted.")

    query := []float32{0.1, 0.2, 0.3, 0.4}

    result, err := points.Search(ctx, &qdrant.SearchPoints{
        CollectionName: "articles",
        Vector:         query,
        Limit:          1,
        WithPayload: &qdrant.WithPayloadSelector{
            SelectorOptions: &qdrant.WithPayloadSelector_Enable{
                Enable: true,
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    if len(result.Result) == 0 {
        fmt.Println("❗ Search returned ZERO results")
        fmt.Println("Full response:", result)
        return
    }

    hit := result.Result[0]
    title := hit.Payload["title"].GetStringValue()

    fmt.Println("Artikel mirip:", title)

    var vectors [][]float32
    var payloads []map[string]interface{}

    for i := 0; i < 1000; i++ {
        v := make([]float32, 128)
        for j := 0; j < 128; j++ {
            v[j] = rand.Float32()
        }
        vectors = append(vectors, v)
        payloads = append(payloads, map[string]interface{}{
            "id": i + 1000,
        })
    }

    _, _ = collections.Delete(ctx, &qdrant.DeleteCollection{CollectionName: "embeddings"})

    _, err = collections.Create(ctx, &qdrant.CreateCollection{
        CollectionName: "embeddings",
        VectorsConfig: &qdrant.VectorsConfig{
            Config: &qdrant.VectorsConfig_Params{
                Params: &qdrant.VectorParams{
                    Size:     128,
                    Distance: qdrant.Distance_Cosine,
                },
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    err = BulkInsert(ctx, points, "embeddings", vectors, payloads)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Bulk insert 1000 embeddings selesai.")
}

func NewVectors(v []float32) *qdrant.Vectors {
    return &qdrant.Vectors{
        VectorsOptions: &qdrant.Vectors_Vector{
            Vector: &qdrant.Vector{Data: v},
        },
    }
}

func NewPoint(id int, vector []float32, title string) *qdrant.PointStruct {
    return &qdrant.PointStruct{
        Id: &qdrant.PointId{
            PointIdOptions: &qdrant.PointId_Num{Num: uint64(id)},
        },
        Vectors: NewVectors(vector),
        Payload: map[string]*qdrant.Value{
            "title": {
                Kind: &qdrant.Value_StringValue{StringValue: title},
            },
        },
    }
}

func BulkInsert(ctx context.Context, client qdrant.PointsClient, col string,
    vectors [][]float32, payloads []map[string]interface{}) error {

    batchSize := 100
    total := len(vectors)
    wg := sync.WaitGroup{}
    errChan := make(chan error, total/batchSize+1)

    for i := 0; i < total; i += batchSize {
        end := i + batchSize
        if end > total {
            end = total
        }

        bvec := vectors[i:end]
        bpay := payloads[i:end]

        wg.Add(1)
        go func(vs [][]float32, ps []map[string]interface{}) {
            defer wg.Done()

            var pts []*qdrant.PointStruct

            for i := range vs {
                id := ps[i]["id"].(int)

                pts = append(pts, &qdrant.PointStruct{
                    Id: &qdrant.PointId{
                        PointIdOptions: &qdrant.PointId_Num{Num: uint64(id)},
                    },
                    Vectors: NewVectors(vs[i]),
                    Payload: map[string]*qdrant.Value{
                        "id": {
                            Kind: &qdrant.Value_IntegerValue{IntegerValue: int64(id)},
                        },
                    },
                })
            }

            _, err := client.Upsert(ctx, &qdrant.UpsertPoints{
                CollectionName: col,
                Points:         pts,
            })
            if err != nil {
                errChan <- err
            }

        }(bvec, bpay)
    }

    wg.Wait()
    close(errChan)

    for e := range errChan {
        return e
    }

    return nil
}
