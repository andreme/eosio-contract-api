import * as express from 'express';

import { AtomicMarketNamespace, SaleApiState } from '../index';
import { HTTPServer } from '../../../server';
import { fillSales } from '../filler';
import { formatSale } from '../format';
import { extendedAssetFilterParameters, atomicDataFilter, baseAssetFilterParameters } from '../../atomicassets/openapi';
import {
    actionGreylistParameters,
    dateBoundaryParameters,
    getOpenAPI3Responses,
    paginationParameters,
    primaryBoundaryParameters
} from '../../../docs';
import { filterQueryArgs } from '../../utils';
import { listingFilterParameters } from '../openapi';
import { buildAssetFilter, buildGreylistFilter, hasAssetFilter } from '../../atomicassets/utils';
import {
    createSocketApiNamespace,
    extractNotificationIdentifiers,
    respondApiError
} from '../../../utils';
import ApiNotificationReceiver from '../../../notification';
import { NotificationData } from '../../../../filler/notifier';
import { OfferState } from '../../../../filler/handlers/atomicassets';
import { SaleState } from '../../../../filler/handlers/atomicmarket';
import QueryBuilder from '../../../builder';
import { getSaleAction, getSaleLogsAction, getSalesAction, getSalesCountAction } from './handlers/sales';

export function salesEndpoints(core: AtomicMarketNamespace, server: HTTPServer, router: express.Router): any {
    const {caching, returnAsJSON} = server.web;

    router.all('/v1/sales', server.web.caching(), returnAsJSON(getSalesAction, core));
    router.all('/v1/sales/_count', server.web.caching(), returnAsJSON(getSalesCountAction, core));

    router.all(['/v1/sales/templates'], server.web.caching(), async (req, res) => {
        try {
            const args = filterQueryArgs(req, {
                symbol: {type: 'string', min: 1},
                collection_name: {type: 'string', min: 1},
                collection_whitelist: {type: 'string', min: 1},

                min_price: {type: 'float', min: 0},
                max_price: {type: 'float', min: 0},

                page: {type: 'int', min: 1, default: 1},
                limit: {type: 'int', min: 1, max: 100, default: 100},
                sort: {
                    type: 'string',
                    values: ['template_id', 'price'],
                    default: 'template_id'
                },
                order: {type: 'string', values: ['asc', 'desc'], default: 'desc'},
            });

            if (!args.symbol) {
                return res.json({success: false, message: 'symbol parameter is required'});
            }

            if (!hasAssetFilter(req) && !args.collection_whitelist) {
                return res.json({success: false, message: 'You need to specify an asset filter!'});
            }

            const query = new QueryBuilder(`
                SELECT DISTINCT ON(asset.contract, asset.template_id) 
                    sale.market_contract, sale.sale_id, asset.contract assets_contract, asset.template_id, price.price
                FROM 
                    atomicmarket_sales sale, atomicassets_offers offer, atomicassets_offers_assets offer_asset, 
                    atomicassets_assets asset, atomicmarket_sale_prices price, atomicassets_templates "template"
            `);

            query.addCondition(`
                sale.assets_contract = offer.contract AND sale.offer_id = offer.offer_id AND
                offer.contract = offer_asset.contract AND offer.offer_id = offer_asset.offer_id AND
                offer_asset.contract = asset.contract AND offer_asset.asset_id = asset.asset_id AND
                asset.contract = "template".contract AND asset.template_id = "template".template_id AND 
                sale.market_contract = price.market_contract AND sale.sale_id = price.sale_id AND 
                asset.template_id IS NOT NULL AND offer_asset.index = 1 AND 
                offer.state = ${OfferState.PENDING.valueOf()} AND sale.state = ${SaleState.LISTED.valueOf()}
            `);

            query.equal('sale.market_contract', core.args.atomicmarket_account);
            query.equal('sale.settlement_symbol', args.symbol);

            if (!args.collection_name) {
                buildGreylistFilter(req, query, {collectionName: 'sale.collection_name'});
            }

            buildAssetFilter(req, query, {assetTable: '"asset"', templateTable: '"template"'});

            if (args.min_price) {
                query.addCondition('price.price >= ' + query.addVariable(args.min_price) + ' * POW(10, price.settlement_precision)');
            }

            if (args.max_price) {
                query.addCondition('price.price <= ' + query.addVariable(args.max_price) + ' * POW(10, price.settlement_precision)');
            }

            if (args.collection_name) {
                query.equalMany('sale.collection_name', args.collection_name.split(','));
            }

            query.append('ORDER BY asset.contract, asset.template_id, price.price ASC');

            const sortColumnMapping: {[key: string]: string} = {
                price: 't1.price',
                template_id: 't1.template_id',
            };

            let queryString = 'SELECT * FROM (' + query.buildString() + ') t1 ';
            queryString += 'ORDER BY ' + sortColumnMapping[args.sort] + ' ' + args.order + ' NULLS LAST, t1.template_id ASC ';
            queryString += 'LIMIT ' + query.addVariable(args.limit) + ' OFFSET ' + query.addVariable((args.page - 1) * args.limit) + ' ';

            const saleResult = await server.query(queryString, query.buildValues());

            const saleLookup: {[key: string]: any} = {};
            const result = await server.query(
                'SELECT * FROM atomicmarket_sales_master WHERE market_contract = $1 AND sale_id = ANY ($2)',
                [core.args.atomicmarket_account, saleResult.rows.map(row => row.sale_id)]
            );

            result.rows.reduce((prev, current) => {
                prev[String(current.sale_id)] = current;

                return prev;
            }, saleLookup);

            const sales = await fillSales(
                server, core.args.atomicassets_account, saleResult.rows.map((row) => formatSale(saleLookup[String(row.sale_id)]))
            );

            res.json({success: true, data: sales, query_time: Date.now()});
        } catch (error) {
            return respondApiError(res, error);
        }
    });

    router.all('/v1/sales/:sale_id', caching(), returnAsJSON(getSaleAction, core));

    router.all('/v1/sales/:sale_id/logs', caching(), returnAsJSON(getSaleLogsAction, core));

    return {
        tag: {
            name: 'sales',
            description: 'Sales'
        },
        paths: {
            '/v1/sales': {
                get: {
                    tags: ['sales'],
                    summary: 'Get all sales. ',
                    description: atomicDataFilter,
                    parameters: [
                        {
                            name: 'state',
                            in: 'query',
                            description: 'Filter by sale state (' +
                                SaleApiState.WAITING.valueOf() + ': WAITING - Sale created but offer was not send yet, ' +
                                SaleApiState.LISTED.valueOf() + ': LISTED - Assets for sale, ' +
                                SaleApiState.CANCELED.valueOf() + ': CANCELED - Sale was canceled, ' +
                                SaleApiState.SOLD.valueOf() + ': SOLD - Sale was bought' +
                                SaleApiState.INVALID.valueOf() + ': INVALID - Sale is still listed but offer is currently invalid (can become valid again if the user owns all assets again)' +
                                ') - separate multiple with ","',
                            required: false,
                            schema: {type: 'string'}
                        },
                        ...listingFilterParameters,
                        ...baseAssetFilterParameters,
                        ...extendedAssetFilterParameters,
                        ...primaryBoundaryParameters,
                        ...dateBoundaryParameters,
                        ...paginationParameters,
                        {
                            name: 'sort',
                            in: 'query',
                            description: 'Column to sort',
                            required: false,
                            schema: {
                                type: 'string',
                                enum: [
                                    'created', 'updated', 'sale_id', 'price',
                                    'template_mint'
                                ],
                                default: 'created'
                            }
                        }
                    ],
                    responses: getOpenAPI3Responses([200, 500], {
                        type: 'array',
                        items: {'$ref': '#/components/schemas/Sale'}
                    })
                }
            },
            '/v1/sales/templates': {
                get: {
                    tags: ['sales'],
                    summary: 'Get the cheapest sale grouped by templates. ',
                    description: atomicDataFilter,
                    parameters: [
                        {
                            name: 'symbol',
                            in: 'query',
                            description: 'Token symbol',
                            required: true,
                            schema: {type: 'string'}
                        },
                        {
                            name: 'min_price',
                            in: 'query',
                            description: 'Min price',
                            required: false,
                            schema: {type: 'number'}
                        },
                        {
                            name: 'max_price',
                            in: 'query',
                            description: 'Max price',
                            required: false,
                            schema: {type: 'number'}
                        },
                        ...baseAssetFilterParameters,
                        ...extendedAssetFilterParameters,
                        ...primaryBoundaryParameters,
                        ...paginationParameters,
                        {
                            name: 'sort',
                            in: 'query',
                            description: 'Column to sort',
                            required: false,
                            schema: {
                                type: 'string',
                                enum: ['template_id', 'price'],
                                default: 'created'
                            }
                        }
                    ],
                    responses: getOpenAPI3Responses([200, 500], {
                        type: 'array',
                        items: {'$ref': '#/components/schemas/Sale'}
                    })
                }
            },
            '/v1/sales/{sale_id}': {
                get: {
                    tags: ['sales'],
                    summary: 'Get a specific sale by id',
                    parameters: [
                        {
                            in: 'path',
                            name: 'sale_id',
                            description: 'Sale Id',
                            required: true,
                            schema: {type: 'integer'}
                        }
                    ],
                    responses: getOpenAPI3Responses([200, 416, 500], {'$ref': '#/components/schemas/Sale'})
                }
            },
            '/v1/sales/{sale_id}/logs': {
                get: {
                    tags: ['sales'],
                    summary: 'Fetch sale logs',
                    parameters: [
                        {
                            name: 'sale_id',
                            in: 'path',
                            description: 'ID of sale',
                            required: true,
                            schema: {type: 'integer'}
                        },
                        ...paginationParameters,
                        ...actionGreylistParameters
                    ],
                    responses: getOpenAPI3Responses([200, 500], {type: 'array', items: {'$ref': '#/components/schemas/Log'}})
                }
            }
        }
    };
}

export function salesSockets(core: AtomicMarketNamespace, server: HTTPServer, notification: ApiNotificationReceiver): void {
    const namespace = createSocketApiNamespace(server, core.path + '/v1/sales');

    notification.onData('sales', async (notifications: NotificationData[]) => {
        const saleIDs = extractNotificationIdentifiers(notifications, 'sale_id');
        const query = await server.query(
            'SELECT * FROM atomicmarket_sales_master WHERE market_contract = $1 AND sale_id = ANY($2)',
            [core.args.atomicmarket_account, saleIDs]
        );

        const sales = await fillSales(server, core.args.atomicassets_account, query.rows.map((row: any) => formatSale(row)));

        for (const notification of notifications) {
            if (notification.type === 'trace' && notification.data.trace) {
                const trace = notification.data.trace;

                if (trace.act.account !== core.args.atomicmarket_account) {
                    continue;
                }

                const saleID = (<any>trace.act.data).sale_id;

                if (trace.act.name === 'lognewsale') {
                    namespace.emit('new_sale', {
                        transaction: notification.data.tx,
                        block: notification.data.block,
                        trace: trace,
                        sale_id: saleID,
                        sale: sales.find((row: any) => String(row.sale_id) === String(saleID))
                    });
                }
            } else if (notification.type === 'fork') {
                namespace.emit('fork', {block_num: notification.data.block.block_num});
            }
        }
    });
}
