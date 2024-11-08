export interface Response {
  status: number;
  body: string;
  timestamp: string;
  headers?: HeadersInit;
}

export interface Request {
  protocol?: string;
  host: string;
  port?: number | string;
  path?: string;
  method?: string;
  headers?: HeadersInit;
  body?: string;
  timestamp: string;
}
export interface Orchestration {
  name: string;
  request: Request;
  response: Response;
}
export interface OpenHimResponseObject {
  'x-mediator-urn': string;
  status: string;
  response: Response;
  orchestrations: Orchestration[];
}

export interface ResponseObject {
  status: number;
  body: object;
}
