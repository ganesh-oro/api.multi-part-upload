import { flatten, safeParse } from 'valibot';
import { VALIDATION_FAILED } from '../constants/appMessages';
import UnprocessableContentException from '../exceptions/unprocessebleContentException';

const validate = (schema: any, data: any) => {
    const validatedData = safeParse(schema, data, { abortPipeEarly: true });
    if (!validatedData.success) {
        const issues = flatten(validatedData.issues);
        throw new UnprocessableContentException(VALIDATION_FAILED, issues.nested);
    } else {
        return validatedData.output
    }
};

export default validate