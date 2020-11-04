import * as express from 'express';
import PQueue from 'p-queue';

import { AtomicAssetsNamespace } from '../index';
import { HTTPServer } from '../../../server';
import { buildAssetFilter, buildGreylistFilter, getLogs, hideOfferAssets } from '../utils';
import { buildBoundaryFilter, filterQueryArgs } from '../../utils';
import logger from '../../../../utils/winston';
import { primaryBoundaryParameters, getOpenAPI3Responses, paginationParameters, dateBoundaryParameters } from '../../../docs';
import { assetFilterParameters, atomicDataFilter, greylistFilterParameters, hideOffersParameters } from '../openapi';
import { fillAssets, FillerHook } from '../filler';

export function buildAssetQueryCondition(
    req: express.Request, varOffset: number,
    options: {assetTable?: string, templateTable?: string, mintTable?: string} = {}
): {values: any[], str: string} {
    const args = filterQueryArgs(req, {
        authorized_account: {type: 'string', min: 1, max: 12},
        only_duplicate_templates: {type: 'bool'},

        template_mint: {type: 'int', min: 1},
        schema_mint: {type: 'int', min: 1},
        collection_mint: {type: 'int', min: 1},

        min_template_mint: {type: 'int', min: 1},
        max_template_mint: {type: 'int', min: 1},
        min_schema_mint: {type: 'int', min: 1},
        max_schema_mint: {type: 'int', min: 1},
        min_collection_mint: {type: 'int', min: 1},
        max_collection_mint: {type: 'int', min: 1}
    });

    let queryString = ' ';
    let queryValues: any[] = [];
    let varCounter = varOffset;

    if (args.authorized_account) {
        queryString += 'AND EXISTS(' +
            'SELECT * FROM atomicassets_collections collection ' +
            'WHERE collection.collection_name = ' + options.assetTable + '.collection_name AND collection.contract = ' + options.assetTable + '.contract ' +
            'AND $' + ++varCounter + ' = ANY(collection.authorized_accounts)' +
            ') ';
        queryValues.push(args.authorized_account);
    }

    if (args.only_duplicate_templates) {
        queryString += 'AND EXISTS (' +
            'SELECT * FROM atomicassets_assets inner_asset ' +
            'WHERE inner_asset.contract = asset.contract AND inner_asset.template_id = ' + options.assetTable + '.template_id ' +
            'AND inner_asset.asset_id < ' + options.assetTable + '.asset_id AND inner_asset.owner = ' + options.assetTable + '.owner' +
            ') AND ' + options.assetTable + '.template_id IS NOT NULL ';
    }

    queryString += hideOfferAssets(req);

    if (options.mintTable) {
        if (args.template_mint) {
            queryString += 'AND ' + options.mintTable + '.template_mint = $' + ++varCounter + ' ';
            queryValues.push(args.template_mint);
        }

        if (args.schema_mint) {
            queryString += 'AND ' + options.mintTable + '.schema_mint = $' + ++varCounter + ' ';
            queryValues.push(args.schema_mint);
        }

        if (args.collection_mint) {
            queryString += 'AND ' + options.mintTable + '.collection_mint = $' + ++varCounter + ' ';
            queryValues.push(args.collection_mint);
        }

        if (args.min_template_mint) {
            queryString += 'AND ' + options.mintTable + '.template_mint >= $' + ++varCounter + ' ';
            queryValues.push(args.min_template_mint);
        }

        if (args.max_template_mint) {
            queryString += 'AND ' + options.mintTable + '.template_mint <= $' + ++varCounter + ' ';
            queryValues.push(args.max_template_mint);
        }

        if (args.min_schema_mint) {
            queryString += 'AND ' + options.mintTable + '.schema_mint >= $' + ++varCounter + ' ';
            queryValues.push(args.min_schema_mint);
        }

        if (args.max_schema_mint) {
            queryString += 'AND ' + options.mintTable + '.schema_mint <= $' + ++varCounter + ' ';
            queryValues.push(args.max_schema_mint);
        }

        if (args.min_collection_mint) {
            queryString += 'AND ' + options.mintTable + '.collection_mint >= $' + ++varCounter + ' ';
            queryValues.push(args.min_collection_mint);
        }

        if (args.max_collection_mint) {
            queryString += 'AND ' + options.mintTable + '.collection_mint <= $' + ++varCounter + ' ';
            queryValues.push(args.max_collection_mint);
        }
    }

    const assetFilter = buildAssetFilter(req, varCounter, {assetTable: options.assetTable, templateTable: options.templateTable});
    queryValues = queryValues.concat(assetFilter.values);
    varCounter += assetFilter.values.length;
    queryString += assetFilter.str;

    const blacklistFilter = buildGreylistFilter(req, varCounter, options.assetTable + '.collection_name');
    queryValues.push(...blacklistFilter.values);
    queryString += blacklistFilter.str;

    return {
        values: queryValues,
        str: queryString
    };
}

export class AssetApi {
    constructor(
        readonly core: AtomicAssetsNamespace,
        readonly server: HTTPServer,
        readonly schema: string,
        readonly assetView: string,
        readonly assetFormatter: (_: any) => any,
        readonly fillerHook?: FillerHook
    ) { }

    endpoints(router: express.Router): any {
        router.all(['/v1/assets', '/v1/assets/_count'], this.server.web.caching(), (async (req, res) => {
            try {
                const args = filterQueryArgs(req, {
                    page: {type: 'int', min: 1, default: 1},
                    limit: {type: 'int', min: 1, max: 1000, default: 100},
                    sort: {type: 'string', min: 1},
                    order: {type: 'string', values: ['asc', 'desc'], default: 'desc'},
                });

                let varCounter = 1;
                let queryString = 'SELECT asset.asset_id FROM atomicassets_assets asset ' +
                    'LEFT JOIN atomicassets_templates "template" ON (' +
                        'asset.contract = template.contract AND asset.template_id = template.template_id' +
                    ') ' +
                    'LEFT JOIN atomicassets_asset_mints mint ON (' +
                        'asset.contract = mint.contract AND asset.asset_id = mint.asset_id' +
                    ') ';

                if (args.sort && args.sort.startsWith('data')) {
                    queryString += 'LEFT JOIN atomicassets_asset_data data_table ON (' +
                            'asset.contract = data_table.contract AND asset.asset_id = data_table.asset_id' +
                        ') ';
                }

                queryString += 'WHERE asset.contract = $1 ';
                let queryValues: any[] = [this.core.args.atomicassets_account];

                const filter = buildAssetQueryCondition(req, varCounter, {
                    assetTable: '"asset"', templateTable: '"template"', mintTable: '"mint"'
                });

                queryString += filter.str;
                varCounter += filter.values.length;
                queryValues = queryValues.concat(filter.values);

                const boundaryFilter = buildBoundaryFilter(
                    req, varCounter,
                    'asset.asset_id', 'int',
                    args.sort === 'updated' ? 'asset.updated_at_time' : 'asset.minted_at_time',
                    args.sort === 'updated' ? 'asset.updated_at_block' : 'asset.minted_at_block'
                );

                queryValues = queryValues.concat(boundaryFilter.values);
                varCounter += boundaryFilter.values.length;
                queryString += boundaryFilter.str;

                if (req.originalUrl.search('/_count') >= 0) {
                    const countQuery = await this.server.query(
                        'SELECT COUNT(*) counter FROM (' + queryString + ') x',
                        queryValues
                    );

                    return res.json({success: true, data: countQuery.rows[0].counter, query_time: Date.now()});
                }

                let sortColumn: string;

                if (args.sort) {
                    if (args.sort.startsWith('data')) {
                        if (args.sort.startsWith('data:text.')) {
                            sortColumn = '"data_table"."data"->>\'' + args.sort.substr('data:text.'.length).replace('\'', '') + '\'';
                        } else if (args.sort.startsWith('data:number.')) {
                            sortColumn = '("data_table"."data"->>\'' + args.sort.substr('data:number.'.length).replace('\'', '') + '\')::double precision';
                        } else {
                            sortColumn = '"data_table"."data"->>\'' + args.sort.substr('data.'.length).replace('\'', '') + '\'';
                        }
                    } else {
                        const sortColumnMapping: {[key: string]: string} = {
                            asset_id: 'asset.asset_id',
                            updated: 'asset.updated_at_block',
                            minted: 'asset.asset_id',
                            collection_mint: 'mint.collection_mint',
                            schema_mint: 'mint.schema_mint',
                            template_mint: 'mint.template_mint'
                        };

                        sortColumn = sortColumnMapping[args.sort];
                    }
                }

                if (!sortColumn) {
                    sortColumn = 'asset.asset_id';
                }

                // @ts-ignore
                queryString += 'ORDER BY ' + sortColumn + ' ' + args.order + ' NULLS LAST, asset.asset_id ASC ';
                queryString += 'LIMIT $' + ++varCounter + ' OFFSET $' + ++varCounter + ' ';
                queryValues.push(args.limit);
                queryValues.push((args.page - 1) * args.limit);

                const query = await this.server.query(queryString, queryValues);

                const assets = await fillAssets(
                    this.server, this.core.args.atomicassets_account,
                    query.rows.map(row => row.asset_id),
                    this.assetFormatter, this.assetView, this.fillerHook
                );

                return res.json({success: true, data: assets, query_time: Date.now()});
            } catch (e) {
                return res.status(500).json({success: false, message: 'Internal Server Error'});
            }
        }));

        router.all('/v1/assets/:asset_id', this.server.web.caching({ignoreQueryString: true}), (async (req, res) => {
            try {
                const assets = await fillAssets(
                    this.server, this.core.args.atomicassets_account,
                    [req.params.asset_id],
                    this.assetFormatter, this.assetView, this.fillerHook
                );

                if (assets.length === 0 || typeof assets[0] === 'string') {
                    return res.status(416).json({success: false, message: 'Asset not found'});
                }

                return res.json({success: true, data: assets[0], query_time: Date.now()});
            } catch (e) {
                return res.status(500).json({success: false, message: 'Internal Server Error'});
            }
        }));

        router.all('/v1/assets/:asset_id/stats', this.server.web.caching({ignoreQueryString: true}), (async (req, res) => {
            try {
                const assetQuery = await this.server.query(
                    'SELECT * FROM atomicassets_assets WHERE contract = $1 AND asset_id = $2',
                    [this.core.args.atomicassets_account, req.params.asset_id]
                );

                if (assetQuery.rowCount === 0) {
                    return res.status(416).json({success: false, message: 'Asset not found'});
                }

                const asset = assetQuery.rows[0];

                const query = await this.server.query(
                    'SELECT COUNT(*) template_mint FROM atomicassets_assets WHERE contract = $1 AND asset_id <= $2 AND template_id = $3 AND schema_name = $4 AND collection_name = $5',
                    [this.core.args.atomicassets_account, asset.asset_id, asset.template_id, asset.schema_name, asset.collection_name]
                );

                return res.json({success: true, data: query.rows[0]});
            } catch (e) {
                res.status(500).json({success: false, message: 'Internal Server Error'});
            }
        }));

        router.all('/v1/assets/:asset_id/logs', this.server.web.caching(), (async (req, res) => {
            const args = filterQueryArgs(req, {
                page: {type: 'int', min: 1, default: 1},
                limit: {type: 'int', min: 1, max: 100, default: 100},
                order: {type: 'string', values: ['asc', 'desc'], default: 'asc'}
            });

            try {
                res.json({
                    success: true,
                    data: await getLogs(
                        this.server, this.core.args.atomicassets_account, 'asset', req.params.asset_id,
                        (args.page - 1) * args.limit, args.limit, args.order
                    ), query_time: Date.now()
                });
            } catch (e) {
                logger.error(e);

                return res.status(500).json({success: false, message: 'Internal Server Error'});
            }
        }));

        return {
            tag: {
                name: 'assets',
                description: 'Assets'
            },
            paths: {
                '/v1/assets': {
                    get: {
                        tags: ['assets'],
                        summary: 'Fetch assets.',
                        description: atomicDataFilter,
                        parameters: [
                            ...assetFilterParameters,
                            {
                                name: 'only_duplicate_templates',
                                in: 'query',
                                description: 'Show only duplicate assets grouped by template',
                                required: false,
                                schema: {
                                    type: 'boolean'
                                }
                            },
                            {
                                name: 'authorized_account',
                                in: 'query',
                                description: 'Filter for assets the provided account can edit. ',
                                required: false,
                                schema: {
                                    type: 'string'
                                }
                            },
                            ...hideOffersParameters,
                            ...greylistFilterParameters,
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
                                    enum: ['asset_id', 'minted', 'updated', 'template_mint'],
                                    default: 'asset_id'
                                }
                            }
                        ],
                        responses: getOpenAPI3Responses([200, 500], {type: 'array', items: {'$ref': '#/components/schemas/' + this.schema}})
                    }
                },
                '/v1/assets/{asset_id}': {
                    get: {
                        tags: ['assets'],
                        summary: 'Fetch asset by id',
                        parameters: [
                            {
                                name: 'asset_id',
                                in: 'path',
                                description: 'ID of asset',
                                required: true,
                                schema: {type: 'string'}
                            }
                        ],
                        responses: getOpenAPI3Responses([200, 416, 500], {'$ref': '#/components/schemas/' + this.schema})
                    }
                },
                '/v1/assets/{asset_id}/stats': {
                    get: {
                        tags: ['assets'],
                        summary: 'Fetch asset stats',
                        parameters: [
                            {
                                name: 'asset_id',
                                in: 'path',
                                description: 'ID of asset',
                                required: true,
                                schema: {type: 'integer'}
                            }
                        ],
                        responses: getOpenAPI3Responses([200, 500], {
                            type: 'object',
                            properties: {
                                template_mint: {type: 'integer'}
                            }
                        })
                    }
                },
                '/v1/assets/{asset_id}/logs': {
                    get: {
                        tags: ['assets'],
                        summary: 'Fetch asset logs',
                        parameters: [
                            {
                                name: 'asset_id',
                                in: 'path',
                                description: 'ID of asset',
                                required: true,
                                schema: {type: 'integer'}
                            },
                            ...paginationParameters
                        ],
                        responses: getOpenAPI3Responses([200, 500], {type: 'array', items: {'$ref': '#/components/schemas/Log'}})
                    }
                }
            }
        };
    }

    sockets(): void {
        const namespace = this.server.socket.io.of(this.core.path + '/v1/assets');

        namespace.on('connection', async (socket) => {
            logger.debug('socket asset client connected');

            let verifiedConnection = false;
            if (!(await this.server.socket.reserveConnection(socket))) {
                socket.disconnect(true);
            } else {
                verifiedConnection = true;
            }

            socket.on('disconnect', async () => {
                if (verifiedConnection) {
                    await this.server.socket.releaseConnection(socket);
                }
            });
        });

        const queue = new PQueue({
            autoStart: true,
            concurrency: 1
        });

        const assetChannelName = [
            'eosio-contract-api', this.core.connection.chain.name, this.core.args.connected_reader,
            'atomicassets', this.core.args.atomicassets_account, 'assets'
        ].join(':');
        this.core.connection.redis.ioRedisSub.setMaxListeners(this.core.connection.redis.ioRedisSub.getMaxListeners() + 1);
        this.core.connection.redis.ioRedisSub.subscribe(assetChannelName, () => {
            this.core.connection.redis.ioRedisSub.on('message', async (channel, message) => {
                if (channel !== assetChannelName) {
                    return;
                }

                const msg = JSON.parse(message);

                logger.debug('received asset notification', msg);

                await queue.add(async () => {
                    const query = await this.server.query(
                        'SELECT * FROM atomicassets_assets_master WHERE contract = $1 AND asset_id = $2',
                        [this.core.args.atomicassets_account, msg.data.asset_id]
                    );

                    if (query.rowCount === 0) {
                        logger.error('Received asset notification but did not find it in database');

                        return;
                    }

                    const asset = query.rows[0];

                    if (msg.action === 'mint') {
                        namespace.emit('new_asset', {
                            transaction: msg.transaction,
                            block: msg.block,
                            asset: this.assetFormatter(asset)
                        });
                    } else if (msg.action === 'burn') {
                        namespace.emit('burn', {
                            transaction: msg.transaction,
                            block: msg.block,
                            asset: this.assetFormatter(asset)
                        });
                    } else if (msg.action === 'back') {
                        namespace.emit('back', {
                            transaction: msg.transaction,
                            block: msg.block,
                            asset: this.assetFormatter(asset),
                            trace: msg.data.trace
                        });
                    } else if (msg.action === 'update') {
                        namespace.emit('update', {
                            transaction: msg.transaction,
                            block: msg.block,
                            asset: this.assetFormatter(asset),
                            delta: msg.data.delta
                        });
                    }
                });
            });
        });

        this.server.socket.addForkSubscription(this.core.args.connected_reader, namespace);
    }
}
