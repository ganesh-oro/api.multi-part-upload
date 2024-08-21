import { serve } from '@hono/node-server'
import { Context, Hono } from 'hono'
import { SOMETHING_WENT_WRONG } from './constants/appMessages'
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { multiPart } from './routes/multipartUpload';
import configData from '../config/app';

const app = new Hono()

app.use("*", cors());

app.use(logger())

app.get('/', (c) => {
  return c.text('Hello Hono!')
})

app.route('/' + configData.app.api_version + '/multipart-upload', multiPart);


console.log(`Server is running on port ${configData.app.port}`)

app.onError((err: any, c: Context) => {
  c.status(err.status || 500)
  return c.json({
    success: false,
    status: err.status || 500, // if you get 500 you are the worst person in the world
    message: err.message || SOMETHING_WENT_WRONG,
    errors: err.errData || null
  })
})

serve({
  fetch: app.fetch,
  port: configData.app.port
})
