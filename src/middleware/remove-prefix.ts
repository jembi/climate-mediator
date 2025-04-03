import { Request, Response, NextFunction } from 'express';

const removePrefixMiddleWare = (prefix: string) => {
  return (req: Request, res: Response, next: NextFunction) => {
    if (req.url.startsWith(prefix)) {
      req.url = req.url.slice(prefix.length);
    }
    next();
  };
};

export default removePrefixMiddleWare;
