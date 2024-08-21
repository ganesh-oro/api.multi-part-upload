import { Context } from "hono";
import { StatusCode } from "hono/utils/http-status";



export function sendSuccessResponse(c: Context, status:StatusCode, message: string = "", data: any = []) {

    let responseBody = {
        success: true,
        message,
        status: status,
        data
    }
    c.status(status);
    return c.json(responseBody);

}