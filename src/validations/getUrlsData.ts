import { InferInput, integer, minLength, nonEmpty, number, object, pipe, string } from 'valibot';
import { FILE_KEY_REQUIRED, FILE_KEY_STRING, PARTS_NUMBER, UPLOAD_ID_REQUIRED, UPLOAD_ID_STRING } from '../constants/appMessages';

export const getUrlsData = object({
    file_key: pipe(
        string(FILE_KEY_STRING),
        nonEmpty(FILE_KEY_REQUIRED)
    ),

    parts: pipe(
        number(PARTS_NUMBER),
        integer(PARTS_NUMBER),
    ),

    upload_id: pipe(
        string(UPLOAD_ID_STRING),
        nonEmpty(UPLOAD_ID_REQUIRED)
    )
});

export type GetURLsDataInput = InferInput<typeof getUrlsData>