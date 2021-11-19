import { DB } from './server';
import { RequestParams } from './namespaces/utils';

export interface ActionHandlerContext<T> {
    pathParams: RequestParams,
    db: DB,
    core: T,
}

export type ActionHandler = (params: RequestParams, ctx: ActionHandlerContext<any>) => Promise<any>;
