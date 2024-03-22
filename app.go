package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	bucket := aws.String("evanmmo-vods")

	accessKey := flag.String("accessKey", "", "AWS Access Key")
	secretKey := flag.String("secretKey", "", "AWS Secret Key")
	outFolder := flag.String("o", "", "Output Folder")
	skipExisting := flag.Bool("skipExisting", false, "Skip existing files")
	flag.Parse()

	if *accessKey == "" || *secretKey == "" {
		log.Fatal("Missing AWS Access Key or Secret Key")
	}

	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		Endpoint: aws.String("https://s3.us-east-005.backblazeb2.com"),
		Region: aws.String("us-east-005"),
		S3ForcePathStyle: aws.Bool(true),
	}

	session, err := session.NewSession(s3Config)

	if err != nil {
		fmt.Println(err)
	}

	s3Client := s3.New(session)
	s3Downloader := s3manager.NewDownloader(session)

	var objects []*s3.Object

	input := &s3.ListObjectsV2Input{
		Bucket: bucket,
	}

	for {
		output, err := s3Client.ListObjectsV2(input)
		if err != nil {
			log.Fatal(err)
		}

		objects = append(objects, output.Contents...)

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}

		if !*output.IsTruncated {
			break
		}

		input.ContinuationToken = output.NextContinuationToken
	}

	log.Printf("Found %d objects", len(objects))

	var wg sync.WaitGroup
	objectChannel := make(chan *s3.Object, len(objects))
	for _, obj := range objects {
		objectChannel <- obj
	}
	close(objectChannel)

	const concurrency = 4
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range objectChannel {
				filePath := path.Join(*outFolder, *obj.Key)
				if *skipExisting {
					if _, err := os.Stat(filePath); !os.IsNotExist(err) {
						log.Printf("Skipping %s", *obj.Key)
						continue
					}
				}
				localFile, err := os.Create(filePath)
				if err != nil {
					log.Fatal(err)
				}

				_, err = s3Downloader.Download(localFile, &s3.GetObjectInput{
					Bucket: bucket,
					Key:    obj.Key,
				})

				if err != nil {
					log.Fatal(err)
				} else {
					log.Printf("Downloaded %s", *obj.Key)
				}

				localFile.Close()
			}
		}()
	}

	wg.Wait()

	log.Println("Done!")
}