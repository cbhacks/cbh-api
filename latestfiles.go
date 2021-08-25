package main

import (
    "sync"
    "regexp"
    "net/http"
    "github.com/gin-gonic/gin"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func init() {
    // Install API endpoints.
    router.GET("/v1/latestfiles/:bucket/:channel", getLatestfile)

    // Scan the table for initial values to populate the cache. This is
    // done to prevent attackers from simply spamming fake file ID's and
    // denying serivce on legitimate ID's.
    res, err := dynDb.Scan(&dynamodb.ScanInput {
        TableName: aws.String("LatestFiles"),
        ProjectionExpression: aws.String("#bkt, Channel"),
        ExpressionAttributeNames: map[string]*string{
            "#bkt": aws.String("Bucket"),
        },
    })
    if err != nil {
        panic(err)
    }
    for _, item := range res.Items {
        var id LatestfileId
        err = dynamodbattribute.UnmarshalMap(item, &id)
        if err != nil {
            panic(err)
        }

        cache := new(LatestfileCacheEntry)
        cache.RateBucket = MakeRateBucket(2, 10)
        latestfilesCache[id] = cache
    }
}

type LatestfileId struct {
    Bucket  string `json:"bucket"`
    Channel string `json:"channel"`
}

type LatestfileInfo struct {
    LatestfileId
    Key      string `json:"key"`
    URL      string `json:"url"`
    BuildNum string `json:"buildnum"`
}

type LatestfileCacheEntry struct {
    Info       *LatestfileInfo
    RateBucket *RateBucket
}

// Rate limit for never-before-seen LatestfileId lookups
var latestfilesUnseenRateBucket = MakeRateBucket(4, 20)

// Cached information for past LatestFiles lookups
var latestfilesCache = make(map[LatestfileId]*LatestfileCacheEntry)
var latestfilesCacheMutex sync.Mutex

// GET /v1/latestfile/:bucket/:channel
func getLatestfile(c *gin.Context) {
    bucket := c.Param("bucket")
    channel := c.Param("channel")

    id := LatestfileId{bucket, channel}

    latestfilesCacheMutex.Lock()
    defer latestfilesCacheMutex.Unlock()

    // Search for this ID in the cache.
    cache := latestfilesCache[id]

    if cache != nil {
        // This is a known latestfile.

        // Apply its rate limit.
        if !cache.RateBucket.TryTake() {
            if cache.Info != nil {
                c.IndentedJSON(http.StatusOK, cache.Info)
                return
            } else {
                c.Status(http.StatusTooManyRequests)
                return
            }
        }
    } else {
        // This latestfile entry has no respective cache entry.

        // Apply new ID rate limit.
        if !latestfilesUnseenRateBucket.TryTake() {
            c.Status(http.StatusTooManyRequests)
            return
        }
    }

    // Fetch the respective row, if it exists, from DynamoDB.
    res, err := dynDb.GetItem(&dynamodb.GetItemInput {
        TableName: aws.String("LatestFiles"),
        Key: map[string]*dynamodb.AttributeValue{
            "Bucket": { S: aws.String(bucket) },
            "Channel": { S: aws.String(channel) },
        },
    })
    if err != nil {
        c.Status(http.StatusInternalServerError)
        return
    }
    if len(res.Item) == 0 {
        c.Status(http.StatusNotFound)
        return
    }

    // Parse the result from the DynamoDB API.
    var v struct {
        ObjectKey string
        Pattern   string
    }
    err = dynamodbattribute.UnmarshalMap(res.Item, &v)
    if err != nil {
        c.Status(http.StatusInternalServerError)
        return
    }

    // Parse the pattern from the object key.
    re, err := regexp.Compile(v.Pattern)
    if err != nil {
        c.Status(http.StatusInternalServerError)
        return
    }
    matches := re.FindSubmatch([]byte(v.ObjectKey))

    // Create the cache entry for this ID if it doesn't exist yet.
    if cache == nil {
        cache = new(LatestfileCacheEntry)
        cache.RateBucket = MakeRateBucket(2, 10)
        latestfilesCache[id] = cache
    }

    // Populate info from the database lookup.
    cache.Info = new(LatestfileInfo)
    cache.Info.LatestfileId = id
    cache.Info.Key = v.ObjectKey
    cache.Info.URL = "https://" + id.Bucket + "/" + v.ObjectKey
    if matches != nil && len(matches) >= 2 {
        cache.Info.BuildNum = string(matches[1])
    }

    // OK. Return newly fetched info.
    c.IndentedJSON(http.StatusOK, cache.Info)
}
