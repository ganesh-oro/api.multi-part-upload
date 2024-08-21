import { InferInput, minLength, nonEmpty, nullish, number, object, pipe, string } from 'valibot';
import { FILE_NAME_IS_STRING, FILE_ORIGINAL_NAME_AT_LEAST_5_CHARACTERS, FILE_ORIGINAL_NAME_REQUIRED, FILE_SIZE_IS_NUMBER, FILE_TYPE_REQUIRED, FILE_TYPE_STRING } from '../constants/appMessages';

export const uploadFileData = object({
  original_name: pipe(
    string(FILE_NAME_IS_STRING),
    nonEmpty(FILE_ORIGINAL_NAME_REQUIRED),
    minLength(5,FILE_ORIGINAL_NAME_AT_LEAST_5_CHARACTERS)
  ),

  file_type: pipe(
    string(FILE_TYPE_STRING),
    nonEmpty(FILE_TYPE_REQUIRED)
    ),

  file_size: nullish(number(FILE_SIZE_IS_NUMBER))
});

export type UploadeFileDataInput = InferInput<typeof uploadFileData>