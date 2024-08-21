import { array, InferInput, integer, minLength, nonEmpty, number, object, pipe, string } from 'valibot';
import { ETAG_REQUIRED, ETAG_STRING, FILE_KEY_REQUIRED, FILE_KEY_STRING, PARTS_NUMBER, UPLOAD_ID_REQUIRED, UPLOAD_ID_STRING } from '../constants/appMessages';

const parts = object({

    ETag: pipe(
        string(ETAG_STRING),
        nonEmpty(ETAG_REQUIRED)
    ),

    PartNumber: pipe(
        number(PARTS_NUMBER),
        integer(PARTS_NUMBER),
    ),


})

export const completeUploadData = object({
    file_key: pipe(
        string(FILE_KEY_STRING),
        nonEmpty(FILE_KEY_REQUIRED)
    ),

    upload_id: pipe(
        string(UPLOAD_ID_STRING),
        nonEmpty(UPLOAD_ID_REQUIRED)
    ),

    parts: pipe(array(parts),minLength(1,'At least one part is required'))
});

export type CompleteUploadDataInput = InferInput<typeof completeUploadData>