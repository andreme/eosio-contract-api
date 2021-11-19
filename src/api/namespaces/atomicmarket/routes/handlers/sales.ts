import { filterQueryArgs, RequestParams } from '../../../utils';
import { fillSales } from '../../filler';
import { formatSale } from '../../format';
import { ApiError } from '../../../../error';
import { AtomicMarketContext } from '../../index';
import { applyActionGreylistFilters, getContractActionLogs } from '../../../../utils';

export async function getSaleAction(params: RequestParams, ctx: AtomicMarketContext): Promise<any> {
    const query = await ctx.db.query(
        'SELECT * FROM atomicmarket_sales_master WHERE market_contract = $1 AND sale_id = $2',
        [ctx.core.args.atomicmarket_account, ctx.pathParams.sale_id]
    );

    if (query.rowCount === 0) {
        throw new ApiError('Sale not found', 416);
    }

    const sales = await fillSales(
        ctx.db, ctx.core.args.atomicassets_account, query.rows.map(formatSale)
    );

    return sales[0];
}

export async function getSaleLogsAction(params: RequestParams, ctx: AtomicMarketContext): Promise<any> {
    const args = filterQueryArgs(params, {
        page: {type: 'int', min: 1, default: 1},
        limit: {type: 'int', min: 1, max: 100, default: 100},
        order: {type: 'string', values: ['asc', 'desc'], default: 'asc'}
    });

    return await getContractActionLogs(
        ctx.db, ctx.core.args.atomicmarket_account,
        applyActionGreylistFilters(['lognewsale', 'logsalestart', 'cancelsale', 'purchasesale'], args),
        {sale_id: ctx.pathParams.sale_id},
        (args.page - 1) * args.limit, args.limit, args.order
    );
}
