export interface MediatorConfig {
  urn: string;
  version: string;
  name: string;
  description: string;
  defaultChannelConfig: ChannelConfig[];
  endpoints: Route[];
  configDefs?: string[];
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
