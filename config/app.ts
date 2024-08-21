import dotenv from 'dotenv';

dotenv.config({ path: '.env' });

// set up data
const appData = {
    port: parseInt(process.env.PORT || '3000'),
    api_version: process.env.API_VERSION || 'v1.0',
};

const s3 = {
    s3_bucket: process.env.AWS_S3_BUCKET, 
    bucket_region: process.env.AWS_S3_BUCKET_REGION, 
    accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID, 
    secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY, 
    expires: 3600
}


const configData = {
    app: appData,
    s3: s3
};

export default configData;  
