import { CompletedPart, CompleteMultipartUploadCommand, CreateMultipartUploadCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import configData from "../../config/app";


interface Config {
    credentials: {
        accessKeyId: string;
        secretAccessKey: string;
    },
    region: string,
    s3_bucket: string,
    expires: number,
    useAccelerateEndpoint?: boolean
}

export class MultipartUploadService {
    config: Config;
    client: S3Client;
    constructor() {

        this.config = {
            credentials: {
                accessKeyId: configData.s3.accessKeyId || '',
                secretAccessKey: configData.s3.secretAccessKey || '',
            },

            region: configData.s3.bucket_region || '',
            s3_bucket: configData.s3.s3_bucket || '',
            expires: 3600
        };

        this.client = new S3Client(this.config);
    }

    async initializeMultipartUpload(slug: string, fileName: string) {

        try {
            let key = ''
            if (slug) {
                key += `${slug}/`
            }
            key += fileName
            // Create a new multipart upload.
            const multipartUpload = await this.client.send(
                new CreateMultipartUploadCommand({
                    Bucket: this.config.s3_bucket,
                    Key: key
                })
            )
    
            return multipartUpload; 
        } catch (error) {
            throw error;
        }

    }

    async multipartPresignedUrl(fileKey: string, parts: number, uploadId: string) {

        try {
            const promises = [];

            for (let i = 0; i < parts; i++) {
                const baseParams = {
                    Bucket: this.config.s3_bucket,
                    Key: fileKey,
                    UploadId: uploadId,
                    PartNumber: i + 1
                };
    
                const presignCommand = new PutObjectCommand(baseParams);
    
                promises.push(await getSignedUrl(this.client, presignCommand));
            }
    
            return await Promise.all(promises); 
        } catch (error) {
            throw error;
        }

    }

    async completeMultipartUpload(fileKey: string, uploadId: string, uploadedParts: CompletedPart[]) {
        const bucketName = this.config.s3_bucket
        const key = fileKey;

        try {
            // Complete the multipart upload.
            const response = await this.client.send(

                new CompleteMultipartUploadCommand({

                    Bucket: bucketName,
                    Key: key,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: uploadedParts,
                    }
                }
                )
            )

            return response;

        } catch (error) {
            throw error;
        }
    }
}