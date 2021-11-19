import { buildBoundaryFilter, filterQueryArgs, RequestValues } from '../../../utils';
import { fillSales } from '../../filler';
import { formatSale } from '../../format';
import { ApiError } from '../../../../error';
import { AtomicMarketContext } from '../../index';
import { applyActionGreylistFilters, getContractActionLogs } from '../../../../utils';
import QueryBuilder from '../../../../builder';
import { buildSaleFilter, hasListingFilter } from '../../utils';
import { buildGreylistFilter, hasAssetFilter, hasDataFilters } from '../../../atomicassets/utils';

export async function getSaleAction(params: RequestValues, ctx: AtomicMarketContext): Promise<any> {
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

export async function getSaleLogsAction(params: RequestValues, ctx: AtomicMarketContext): Promise<any> {
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

export async function getSalesAction(params: RequestValues, ctx: AtomicMarketContext): Promise<any> {
    const args = filterQueryArgs(params, {
        page: {type: 'int', min: 1, default: 1},
        limit: {type: 'int', min: 1, max: 100, default: 100},
        collection_name: {type: 'string', min: 1},
        state: {type: 'string', min: 1},
        sort: {
            type: 'string',
            values: [
                'created', 'updated', 'sale_id', 'price',
                'template_mint'
            ],
            default: 'created'
        },
        order: {type: 'string', values: ['asc', 'desc'], default: 'desc'},
        count: {type: 'bool'}
    });

    const query = new QueryBuilder(`
                SELECT listing.sale_id 
                FROM atomicmarket_sales listing 
                    JOIN atomicassets_offers offer ON (listing.assets_contract = offer.contract AND listing.offer_id = offer.offer_id)
                    LEFT JOIN atomicmarket_sale_prices price ON (price.market_contract = listing.market_contract AND price.sale_id = listing.sale_id)
            `);

    query.equal('listing.market_contract', ctx.core.args.atomicmarket_account);

    buildSaleFilter(params, query);

    if (!args.collection_name) {
        buildGreylistFilter(params, query, {collectionName: 'listing.collection_name'});
    }

    buildBoundaryFilter(
        params, query, 'listing.sale_id', 'int',
        args.sort === 'updated' ? 'listing.updated_at_time' : 'listing.created_at_time'
    );

    if (args.count) {
        const countQuery = await ctx.db.query(
            'SELECT COUNT(*) counter FROM (' + query.buildString() + ') x',
            query.buildValues()
        );

        return countQuery.rows[0].counter;
    }

    const sortMapping: {[key: string]: {column: string, nullable: boolean, numericIndex: boolean}}  = {
        sale_id: {column: 'listing.sale_id', nullable: false, numericIndex: true},
        created: {column: 'listing.created_at_time', nullable: false, numericIndex: true},
        updated: {column: 'listing.updated_at_time', nullable: false, numericIndex: true},
        price: {column: args.state === '3' ? 'listing.final_price' : 'price.price', nullable: true, numericIndex: false},
        template_mint: {column: 'LOWER(listing.template_mint)', nullable: true, numericIndex: false}
    };

    const ignoreIndex = (hasAssetFilter(params) || hasDataFilters(params) || hasListingFilter(params)) && sortMapping[args.sort].numericIndex;

    query.append('ORDER BY ' + sortMapping[args.sort].column + (ignoreIndex ? ' + 1 ' : ' ') + args.order + ' ' + (sortMapping[args.sort].nullable ? 'NULLS LAST' : '') + ', listing.sale_id ASC');
    query.append('LIMIT ' + query.addVariable(args.limit) + ' OFFSET ' + query.addVariable((args.page - 1) * args.limit));

    const saleQuery = await ctx.db.query(query.buildString(), query.buildValues());

    const saleLookup: {[key: string]: any} = {};
    const result = await ctx.db.query(
        'SELECT * FROM atomicmarket_sales_master WHERE market_contract = $1 AND sale_id = ANY ($2)',
        [ctx.core.args.atomicmarket_account, saleQuery.rows.map(row => row.sale_id)]
    );

    result.rows.reduce((prev, current) => {
        prev[String(current.sale_id)] = current;

        return prev;
    }, saleLookup);

    return await fillSales(
        ctx.db, ctx.core.args.atomicassets_account, saleQuery.rows.map((row) => formatSale(saleLookup[String(row.sale_id)]))
    );
}

export async function getSalesCountAction(params: RequestValues, ctx: AtomicMarketContext): Promise<any> {
    return await getSalesAction({...params, count: 'true'}, ctx);
}
