export interface MediatorConfig {
  urn: string;
  version: string;
  name: string;
  description: string;
  defaultChannelConfig: ChannelConfig[];
  endpoints: Route[];
  configDefs?: ConfigDef[];
  config?: Config;
}

interface ConfigDef {
  param: string;
  displayName: string;
  description: string;
  type: string;
  values: any[];
  template: Template[];
  array: boolean;
  _id: string;
}

interface Template {
  param: string;
  displayName: string;
  type: string;
  optional?: boolean;
}

interface Config {
  minio_buckets_registry: MinioBucketsRegistry[];
}

export interface MinioBucketsRegistry {
  bucket: string;
  region?: string;
}

interface ChannelConfig {
  name: string;
  urlPattern: string;
  routes: Route[];
  allow: string[];
  methods: string[];
  type: string;
}

interface Route {
  name: string;
  host: string;
  path?: string;
  port: string;
  primary: boolean;
  type: string;
}
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';
