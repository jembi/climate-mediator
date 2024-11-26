export interface Mediator {
  _id: string;
  urn: string;
  version: string;
  name: string;
  description: string;
  endpoints: Endpoint[];
  defaultChannelConfig: DefaultChannelConfig[];
  configDefs: ConfigDef[];
  __v: number;
  _lastHeartbeat: Date;
  _uptime: number;
  _configModifiedTS: Date;
  config?: Config;
}

interface Config {
  minio_buckets_registry: MinioBucketsRegistry[];
}

export interface MinioBucketsRegistry {
  bucket: string;
  region?: string;
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

interface DefaultChannelConfig {
  name: string;
  urlPattern: string;
  isAsynchronousProcess: boolean;
  methods: string[];
  type: string;
  allow: string[];
  whitelist: any[];
  authType: string;
  routes: Endpoint[];
  matchContentTypes: any[];
  properties: any[];
  txViewAcl: any[];
  txViewFullAcl: any[];
  txRerunAcl: any[];
  status: string;
  rewriteUrls: boolean;
  addAutoRewriteRules: boolean;
  autoRetryEnabled: boolean;
  autoRetryPeriodMinutes: number;
  _id: string;
  alerts: any[];
  rewriteUrlsConfig: any[];
}

interface Endpoint {
  name: string;
  type: string;
  status: string;
  host: string;
  port: number;
  primary: boolean;
  forwardAuthHeader: boolean;
  _id: string;
  path?: string;
}
