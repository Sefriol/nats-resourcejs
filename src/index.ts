import jsonpatch, { AddOperation, CopyOperation, GetOperation, MoveOperation, RemoveOperation, ReplaceOperation, TestOperation } from 'fast-json-patch';
import { parse, ParsedQs } from 'qs';
import { MethodOptions } from './interfaces';
import { Model, Query, Document, Aggregate, ObjectId, ClientSession, LeanDocument } from 'mongoose';
import { Msg, NatsConnection, StringCodec, Codec, NatsError } from 'nats'

const debug = {
  query: require('debug')('resourcejs:query'),
  index: require('debug')('resourcejs:index'),
  get: require('debug')('resourcejs:get'),
  put: require('debug')('resourcejs:put'),
  post: require('debug')('resourcejs:post'),
  patch: require('debug')('resourcejs:patch'),
  delete: require('debug')('resourcejs:delete'),
  virtual: require('debug')('resourcejs:virtual'),
  respond: require('debug')('resourcejs:respond'),
};
import * as utils from './utils';
import { InvalidResourceConfigurationError } from './errors'

class ModelQuery extends Query<any,any> {
  model: Model<Document>
  _conditions?: Object
  options?: any
  _fields?: any
}

interface AggregateModel extends Model<any> {
  pipeline?: Array<any>
}

interface State {
  resource?: any,
  writeOptions?: any,
  findQuery?: any,
  countQuery?: ModelQuery | AggregateModel,
  query?: ModelQuery | AggregateModel,
  queryExec?: ModelQuery,
  skipResource?: Boolean,
  skipDelete?: Boolean,
  modelQuery?: ModelQuery,
  model?: Model<Document>,
  populate?: string,
  item?: LeanDocument<any> | Document | Document[],
  search?: any,
  session?: ClientSession,
  __rMethod?: 'index' | 'get' | 'post' | 'put' | 'patch' | 'delete' | 'virtual'
}

interface ParsedMsg extends Msg {
  parsed: RequestType;
}

interface Request {
  state: State,
  msg: ParsedMsg
}

/* RequestType
  https://resgate.io/docs/specification/res-service-protocol/#request-types
*/
interface RequestType {
  Subject?: string,
  query?: string | ParsedQs,
  token?: any,
  cid?: string,
  params?: any,
  header?: any,
  host?: string,
  remoteAddr?: string,
  uri?: string
}

interface Middlewares {
  before: Function,
  beforeQuery: Function,
  query: Function,
  afterQuery: Function,
  after: Function
}


class Resource {
  app: any;
  nats: NatsConnection;
  options: any;
  name: any;
  model: Model<Document, any>;
  modelName: any;
  route: string;
  methods: string[];
  _swagger: null;
  router: any;
  __swagger: any;
  sc: Codec<String>;
  constructor(app: any, route: String, modelName: String, model: Model<any>, options: any) {
    this.app = app;
    this.nats = app.nats;
    if (options.convertIds === true) {
      options.convertIds = /(^|\.)_id$/;
    }
    this.options = options || {};
    this.name = modelName.toLowerCase();
    this.model = model;
    this.modelName = modelName;
    this.route = `${route}.${this.name}`;
    this.methods = [];
    this._swagger = null;
    this.sc = StringCodec()
  }

  /**
   * Register a new callback but add before and after options to the middleware.
   *
   * @param method, string, GET, POST, PUT, PATCH, DEL
   * @param path, string, url path to the resource
   * @param middlewares, object, contains beforeQuery, afterQuery and select middlewares
   * @param options, object, contains before, after and hook handlers
   */
  private register(method: string, path: string, middlewares: Middlewares, options: any) {
    const { before, beforeQuery, query, afterQuery, after } = middlewares;

    // The fallback error handler.
    const middlewareStack = utils.compose([
      before,
      beforeQuery,
      options.hooks[method].before.bind(this),
      query,
      options.hooks[method].after.bind(this),
      afterQuery,
      after,
    ]);
    const callback = async (error: NatsError | null, msg: Msg) => {
      if (error) {
        console.error(error)
        return
      }
      let parsed: ParsedMsg
      let request: Request
      try {
        parsed = { ...msg, parsed: JSON.parse(this.sc.decode(msg.data).toString()) }
        if (typeof parsed.parsed.query === 'string') parsed.parsed.query = parse(parsed.parsed.query)
        request = { msg: parsed, state: {} }
        try {
          await middlewareStack.call(this, request, this.respond)
        } catch (err) {
          this.respond({ msg: parsed, state: { resource: { status: 500, error: {errors: [err]} } } })
        }
      } catch (error) {
        console.error(error)
      }
    };
    // Add a stack processor so this stack can be executed independently.
    utils.set(this.app.context, ['resourcejs', path, method], middlewareStack);

    // Apply these callbacks to the application.
    switch (method) {
      case 'get':
        this.nats.subscribe(`get.${path}.*`, { callback });
        break;
      case 'index':
        this.nats.subscribe(`get.${path}`, { callback });
        break;
      case 'virtual':
      case 'post':
      case 'put':
      case 'patch':
      case 'delete':
        this.nats.subscribe(`call.${path}.${method}`, { callback })
        break;
    };
  }

  private generateMiddleware(options: MethodOptions, position: 'before' | 'after') {
    let middlewareArray: Array<Function> = [];

    if (options?.[position]) {
      middlewareArray = options[position].map((m) => m.bind(this));
    }
    return utils.compose(middlewareArray);
  }

  /**
   * Sets the different responses and calls the next middleware for
   * execution.
   *
   * @param res
   *   The response to send to the client.
   * @param next
   *   The next middleware
   */
  async respond(req: Request) {
    if (req.state.resource) {
      switch (req.state.resource.status) {
        case 404:
          req.msg.respond(this.sc.encode(JSON.stringify({
            error: {
              code: 'system.notFound',
              message: 'Resource not found'
            }
          })))
          break;
        case 400:
        case 500:
          for (const property in req.state.resource.error.errors) {
            // eslint-disable-next-line max-depth
            if (Object.prototype.hasOwnProperty.call(req.state.resource.error.errors, property)) {
              const error = req.state.resource.error.errors[property];
              const { path, name, message } = error;
              req.state.resource.error.errors[property] = { path, name, message };
            }
          }
          req.msg.respond(this.sc.encode(JSON.stringify({
            error: {
              code: 'system.internalError',
              message: req.state.resource.error.message,
              errors: req.state.resource.error.errors,
            }
          })))
          break;
        case 204:
          // Convert 204 into 200, to preserve the empty result set.
          // Update the empty response body based on request method type.
          debug.respond(`204 -> ${req.state.__rMethod}`);
          switch (req.state.__rMethod) {
            case 'index':
              req.msg.respond(this.sc.encode(JSON.stringify({
                result: {
                  collection: []
                }
              })))
              break;
            default:
              req.msg.respond(this.sc.encode(JSON.stringify({
                result: {
                  model: {}
                }
              })))
              break;
          }
          break;
        default:
          const subject = req.msg.parsed.Subject?.match(`^call\.(.*)\.${req.state.__rMethod}$`)?.[0]
          switch (req.state.__rMethod) {
            case 'delete':
              this.nats.publish("system.reset", this.sc.encode(JSON.stringify({
                'resources': [`${this.route}`]
              })));
              break;
            case 'post':
            case 'put':
            case 'patch':
              this.nats.publish(`event.${subject}.change`, this.sc.encode(JSON.stringify({
                values: req.state.resource.item
              })));
              this.nats.publish("system.reset", this.sc.encode(JSON.stringify({
                'resources': [`${this.route}`]
              })));
              break;
            case 'get':
            case 'index':
            case 'virtual':
              break;
          };
          req.msg.respond(this.sc.encode(JSON.stringify({
            result: {
              model: req.state.resource.item
            }
          })));
          break;
      }
    }
  }

  /**
   * Returns the method options for a specific method to be executed.
   * @param {string} method
   * @param {object | undefined | MethodOptions} options
   * @returns {MethodOptions}
   */

  static getMethodOptions(method: string, options: any | MethodOptions): MethodOptions {
    if (!options) {
      options = {};
    }

    // If this is already converted to method options then return.
    if (options.methodOptions) {
      return options;
    }

    // Uppercase the method.
    method = method.charAt(0).toUpperCase() + method.slice(1).toLowerCase();
    // Find all of the options that may have been passed to the rest method.
    const beforeHandlers: Array<Function> = options.before ?
      (
        Array.isArray(options.before) ? options.before : [options.before]
      ) :
      [];
    const beforeMethodHandlers: Array<Function> = options[`before${method}`] ?
      (
        Array.isArray(options[`before${method}`]) ? options[`before${method}`] : [options[`before${method}`]]
      ) :
      [];
    const before = [...beforeHandlers, ...beforeMethodHandlers];

    const afterHandlers = options.after ?
      (
        Array.isArray(options.after) ? options.after : [options.after]
      ) :
      [];
    const afterMethodHandlers = options[`after${method}`] ?
      (
        Array.isArray(options[`after${method}`]) ? options[`after${method}`] : [options[`after${method}`]]
      ) :
      [];
    const after = [...afterHandlers, ...afterMethodHandlers];
    const methodOptions: MethodOptions = { methodOptions: true, before, after };
    // Expose mongoose hooks for each method.
    ['before', 'after'].forEach((type) => {
      const path = `hooks.${method.toString().toLowerCase()}.${type}`;

      utils.set(
        methodOptions,
        path,
        utils.get(options, path, async(req: Request, next: Function) => {
          return await next();
        })
      );
    });

    // Return the options for this method.
    return methodOptions;
  }

  /**
   * register the whole REST api for this resource.
   *
   * @param {any | undefined} options
   * @returns {*|null|HttpPromise}
   */
  rest(options: any | undefined): Resource {
    return this
      .index(options)
      .get(options)
      .virtual(options)
      .put(options)
      .patch(options)
      .post(options)
      .delete(options);
  }

  /**
   * Get the find query for the index.
   *
   * @param {RequestType} request request object
   * @param {MethodOptions} options options given for the method
   * @param {ModelQuery["_conditions"]} existing existing filter conditions
   * @returns {Object | undefined}
   */
  getFindQuery(query: ParsedQs, options: MethodOptions, existing: ModelQuery["_conditions"]): object | undefined {
    interface Filter {
      name: string,
      selector: string
    }
    const findQuery: any = {};
    options = options || this.options;

    // Get the filters and omit the limit, skip, select, sort and populate.
    const { limit, skip, select, sort, populate, ...filters } = query;

    const setFindQuery = function (name: string, value: string | RegExp | string[] | number | boolean | ObjectId | Date | null | undefined ) {
      // Ensure we do not override any existing query parameters.
      if (!existing || !existing.hasOwnProperty(name)) {
        findQuery[name] = value;
      }
    };

    // Iterate through each filter.
    for (let [name, value] of Object.entries(filters)) {
      if (value !== null && typeof value === 'object') continue
      let queryValue: string | string[] | number | boolean | ObjectId | Date | null | undefined
      // Get the filter object.
      const filter: Filter = utils.zipObject(['name', 'selector'], name.split('__'));

      // See if this parameter is defined in our model.
      const param = this.model.schema.paths[filter.name.split('.')[0]];
      if (param) {
        // See if this selector is a regular expression.
        if (filter.selector === 'regex' && typeof value === 'string') {
          // Set the regular expression for the filter.
          const parts = value.match(/\/?([^/]+)\/?([^/]+)?/);
          let regex = null;
          try {
            regex = new RegExp(parts![1], (parts![2] || 'i'));
          }
          catch (err) {
            debug.query(err);
            regex = null;
          }
          if (regex) {
            setFindQuery(filter.name, regex);
          }
          return;
        } // See if there is a selector.
        else if (filter.selector) {
          var filterQuery = findQuery[filter.name];
          // Init the filter.
          if (!filterQuery) {
            filterQuery = {};
          }

          if (filter.selector === 'exists') {
            switch (value) {
              case 'true':
              case '1':
                queryValue = true
                break;
              case 'true':
              case '1':
                queryValue = false
              default:
                queryValue = !!value
                break;
            }
          }
          // Special case for in filter with multiple values.
          else if (['in', 'nin'].includes(filter.selector)) {
            queryValue = Array.isArray(value) ? value : value!.split(',');
            // @ts-ignore queryValue is always string
            queryValue = queryValue.map((item: string) => utils.getQueryValue(filter.name, item, param, options, filter.selector));
          }
          else {
            // @ts-ignore Set the selector for this filter name.
            queryValue = utils.getQueryValue(filter.name, value, param, options, filter.selector);
          }

          filterQuery[`$${filter.selector}`] = value;
          setFindQuery(filter.name, filterQuery);
          return;
        }
        else {
          // Set the find query to this value.
          queryValue = utils.getQueryValue(filter.name, value, param, options, filter.selector);
          setFindQuery(filter.name, queryValue);
          return;
        }
      }

      if (!options.queryFilter) {
        // Set the find query to this value.
        setFindQuery(filter.name, queryValue);
      }
    }

    // Return the findQuery.
    return findQuery;
  }
  
  countQuery(query: ModelQuery, pipeline: Array<any> | undefined) {
    // We cannot use aggregation if mongoose special options are used... like populate.
    if (!utils.isEmpty(query._mongooseOptions) || !pipeline) {
      return query;
    }
    const stages = [
      { $match: query.getQuery() },
      ...pipeline,
      {
        $group: {
          _id: null,
          count: { $sum: 1 },
        },
      },
    ];
    this.model.aggregate()
    return {
      async countDocuments() {
        const items = await query.model!.aggregate(stages).exec();
        return items.length ? items[0].count : 0;
      },
    };
  }

  indexQuery(query: ModelQuery, pipeline: AggregateModel["pipeline"] | undefined) {
    // We cannot use aggregation if mongoose special options are used... like populate.
    if (!utils.isEmpty(query._mongooseOptions) || !pipeline) {
      return query.lean();
    }

    const stages = [
      { $match: query.getQuery() },
      ...pipeline,
    ];

    if (query.options && query.options.sort && !utils.isEmpty(query.options.sort)) {
      stages.push({ $sort: query.options.sort });
    }
    if (query.options && query.options.skip) {
      stages.push({ $skip: query.options.skip });
    }
    if (query.options && query.options.limit) {
      stages.push({ $limit: query.options.limit });
    }
    if (!utils.isEmpty(query._fields)) {
      stages.push({ $project: query._fields });
    }
    return query.model.aggregate(stages);
  }

  /**
   * The index for a resource.
   *
   * @param options
   */
  index(options: any) {
    options = Resource.getMethodOptions('index', options);
    this.methods.push('index');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);

    // eslint-disable-next-line max-statements
    const beforeQuery = async(req: Request, next: Function) => {
      debug.index('beforeQueryMiddleWare');

      const requestData: RequestType = req.msg.parsed
      const queryURL: ParsedQs | undefined = typeof requestData.query === 'string' ? parse(requestData.query) : requestData.query
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'index';

      // Allow before handlers the ability to disable resource CRUD.
      if (req.state.skipResource) {
        debug.index('Skipping Resource');
        return await last(req);
      }

      // Get the query object.
      const countQuery: ModelQuery | AggregateModel = req.state.countQuery || req.state.modelQuery || req.state.model || this.model;
      if (countQuery instanceof Aggregate) throw new InvalidResourceConfigurationError('countQuery cannot be an aggregate')
      req.state.query = req.state.modelQuery || req.state.model || this.model;
      if (req.state.query instanceof Aggregate) throw new InvalidResourceConfigurationError('(req.state.modelQuery || req.state.model || this.model) cannot result an aggregate')

      // Get the find query.
      if (requestData.query && typeof requestData.query !== 'string') {
        req.state.findQuery = this.getFindQuery(
          requestData.query,
          options,
          req.state.query instanceof ModelQuery ? req.state.query._conditions : {}
        );
      }
      

      // First get the total count.
      let count: number;
      try {
        count = await this.countQuery(countQuery.find(req.state.findQuery) as ModelQuery, req.state.query instanceof ModelQuery ? undefined : req.state.query.pipeline ).countDocuments();
      }
      catch (err) {
        debug.index(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }

      // Get the default limit.
      const defaults = { limit: 10, skip: 0 };
      let { limit, skip } = requestData.query as ParsedQs;
      let parsedLimit: number, parsedSkip: number
      //@ts-ignore
      parsedLimit = parseInt(limit, 10);
      parsedLimit = (isNaN(parsedLimit) || (parsedLimit < 0)) ? defaults.limit : parsedLimit;
      //@ts-ignore
      parsedSkip = parseInt(skip, 10);
      parsedSkip = (isNaN(parsedSkip) || (parsedSkip < 0)) ? defaults.skip : parsedSkip;
      const reqQuery = { limit: parsedLimit, skip: parsedSkip };

      // If a skip is provided, then set the range headers.
      if (reqQuery.skip && !requestData.header.range) {
        requestData.header['range-unit'] = 'items';
        requestData.header.range = `${reqQuery.skip}-${reqQuery.skip + (reqQuery.limit - 1)}`;
      }

      // Get the page range.
      const pageRange = utils.paginate(requestData, count, reqQuery.limit) || {
        limit: reqQuery.limit,
        skip: reqQuery.skip,
      };

      // Make sure that if thsere is a range provided in the headers, it takes precedence.
      if (requestData.header.range) {
        reqQuery.limit = pageRange.limit;
        reqQuery.skip = pageRange.skip;
      }

      // Next get the items within the index.
      req.state.queryExec = req.state.query
        .find(req.state.findQuery)
        .limit(reqQuery.limit)
        .skip(reqQuery.skip)
        .select(utils.getParamQuery(queryURL, 'select'))
        .sort(utils.getParamQuery(queryURL, 'sort')) as ModelQuery;

      // Only call populate if they provide a populate query.
      req.state.populate = utils.getParamQuery(queryURL, 'populate');
      if (req.state.populate) {
        debug.index(`Populate: ${req.state.populate}`);
        req.state.queryExec!.populate(req.state.populate);
      }

      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.index('queryMiddleware');
      try {
        const items = await this.indexQuery(req.state.queryExec!, req.state.query instanceof ModelQuery ? undefined : req.state.query!.pipeline ).exec();
        debug.index(items);
        req.state.item = items;
      }
      catch (err) {
        debug.index(err);
        debug.index(err.name);

        if (err.name === 'CastError' && req.state.populate) {
          err.message = `Cannot populate "${req.state.populate}" as it is not a reference in this resource`;
          debug.index(err.message);
        }

        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      return await next();
    };
    const afterQuery = async(req: Request, next: Function) => {
      debug.index('afterQueryMiddleWare');
      req.state.resource = { item: req.state.item };
      return await next();
    };
    const middlewares: Middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery: beforeQuery,
      query: query,
      afterQuery: afterQuery,
      after: last,
    };

    this.register('index', this.route, middlewares, options);
    return this;
  }

  /**
   * Register the GET method for this resource.
   */
  get(options: any) {
    options = Resource.getMethodOptions('get', options);
    this.methods.push('get');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);

    const beforeQuery = async (req: Request, next: Function) => {
      debug.get('beforeQueryMiddleware');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'get';
      if (req.state.skipResource) {
        debug.get('Skipping Resource');
        return await last(req);
      }
      const requestData: RequestType = req.msg.parsed
      const queryURL: ParsedQs | undefined = typeof requestData.query === 'string' ? parse(requestData.query) : requestData.query

      req.state.query = (req.state.modelQuery || req.state.model || this.model).findOne() as ModelQuery;
      req.state.search = { '_id': req.msg.parsed!.params![`${this.name}Id`] };
      // Only call populate if they provide a populate query.
      const populate = utils.getParamQuery(queryURL, 'populate');
      if (populate) {
        debug.get(`Populate: ${populate}`);
        req.state.query.populate(populate);
      }
      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.get('queryMiddleWare');
      try {
        req.state.item = await req.state.query!.where(req.state.search).lean().exec();
      }
      catch (err) {
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      if (!req.state.item) {
        req.state.resource = { status: 404 };
        return await last(req);
      }
      return await next();
    };

    const select = async(req: Request, next: Function) => {
      debug.get('afterMiddleWare (selectMW)');
      let item = req.state.item as LeanDocument<any>
      if (item === undefined || Array.isArray(item)) {
        req.state.resource = { status: 404 };
        return await last(req);
      }
      // Allow them to only return specified fields.
      const requestData: RequestType = req.msg.parsed
      const queryURL: ParsedQs | undefined = typeof requestData.query === 'string' ? parse(requestData.query) : requestData.query

      const select = utils.getParamQuery(queryURL, 'select');
      if (select) {
        const newItem: any = {};
        // Always include the _id.
        if (item?._id) {
          newItem._id = item._id;
        }
        select.split(' ').map((key: string) => {
          key = key.trim();
          if (Object.prototype.hasOwnProperty.call(item, key)) {
            newItem[key] = item![key];
          }
        });
        item = newItem;
      }
      req.state.resource = { status: 200, item: item };
      return await next();
    };
    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery: select,
      after: last,
    };
    this.register('get', `${this.route}/:${this.name}Id`, middlewares, options);
    return this;
  }

  /**
   * Virtual (GET) method. Returns a user-defined projection (typically an aggregate result)
   * derived from this resource
   * The virtual method expects at least the path and the before option params to be set.
   */
  virtual(options: any) {
    if (!options || !options.path || !options.before) return this;
    const path = options.path;
    options = Resource.getMethodOptions('virtual', options);
    this.methods.push(`virtual/${path}`);

    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);
    const beforeQuery = async(req: Request, next: Function) => {
      debug.virtual('queryMiddleWare');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'virtual';

      if (req.state.skipResource) {
        debug.virtual('Skipping Resource');
        return await last(req);
      }
      req.state.query = req.state.modelQuery || req.state.model;
      if (!req.state.query) {
        req.state.resource = { status: 404 };
      }

      return await next();
    };

    const query = async(req: Request, next: Function) => {
      try {
        const query = req.state.query as ModelQuery
        const item = await query.exec();
        if (!item) req.state.resource = { status: 404 };
        else req.state.resource = { status: 200, item };
      }
      catch (err) {
        req.state.resource = { status: 400, error: err };
      }
      return await next();
    };
    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery: beforeQuery,
      after: last,
    };
    this.register('virtual', `${this.route}/virtual/${path}`, middlewares, options);
    return this;
  }

  /**
   * Post (Create) a new item
   */
  post(options: any) {
    options = Resource.getMethodOptions('post', options);
    this.methods.push('post');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);

    const beforeQuery = async(req: Request, next: Function) => {
      debug.post('beforeQueryMiddleWare');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'post';

      if (req.state.skipResource) {
        debug.post('Skipping Resource');
        return await last(req);
      }

      const Model = req.state.model || this.model;
      if (Array.isArray(req.msg.parsed) && req.msg.parsed.length) {
        req.state.item = req.msg.parsed.map((model) => new Model(model));
      }
      else {
        req.state.item = new Model(req.msg.parsed.params);
      }
      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.post('queryMiddleWare');
      const writeOptions = req.state.writeOptions || {};
      try {
        if (Array.isArray(req.state.item)) {
          if (req.state?.session?.constructor?.name  === 'ClientSession') req.state.session = await this.model.startSession();
          if (!req.state.session!.inTransaction()) await req.state.session!.startTransaction();
          writeOptions.session = req.state.session;
          const errors: Error[] = [];
          let result: Document[]
          await Promise.all(req.state.item.map(item => item.save(writeOptions)))
            .catch((err: Error) => {
              errors.push(err);
            })
            .then((res) => {
              if(res) result = res
            })
            .finally(() => {
              if (errors.length) {
                const err = new Error(`Error${errors.length > 1 ? 's' : ''} occured while trying to save document into database`);
                err.name = 'DatabaseError';
                err.errors = errors;
                throw err;
              }
            });
          await req.state.session!.commitTransaction();
        }
        else {
          req.state.item = await req.state.item!.save(writeOptions);
        }
        debug.post(req.state.item);
      }
      catch (err) {
        debug.post(err);
        if (err.name === 'DatabaseError') {
          if (req.state.session?.inTransaction()) await req.state.session.abortTransaction();
          await req.state.session?.endSession();
        }
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      return await next();
    };

    const afterQuery = async(req: Request, next: Function) => {
      debug.post('afterQueryMiddleWare');
      req.state.resource = { status: 201, item: req.state.item };
      return await next();
    };

    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery,
      after: last,
    };
    this.register('post', this.route, middlewares, options);
    return this;
  }

  /**
   * Put (Update) a resource.
   */
  put(options: any) {
    options = Resource.getMethodOptions('put', options);
    this.methods.push('put');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);
    const beforeQuery = async(req: Request, next: Function) => {
      debug.put('beforeQueryMiddleWare');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'put';

      if (req.state.skipResource) {
        debug.put('Skipping Resource');
        return await last(req);
      }
      const id = req.msg.parsed.Subject?.match(`^call\.${this.route}\.(.*)\.${req.state.__rMethod}$`)?.[0]
      if (!id) {
        const err = new Error('ID Not Found')
        debug.put(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      // Remove __v field
      const { __v, ...update } = req.msg.parsed.params;
      req.state.query = req.state.modelQuery || req.state.model || this.model;
      try {
        req.state.item = await req.state.query.findOne({ _id: id }).exec() as Document<any> | LeanDocument<any>;
      }
      catch (err) {
        debug.put(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      if (!req.state.item) {
        debug.put(`No ${this.name} found with ${this.name}Id: ${id}`);
        req.state.resource = { status: 404 };
        return await last(req);
      }
      req.state.item.set(update);
      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.put('queryMiddleWare');
      const writeOptions = req.state.writeOptions || {};
      try {
        req.state.item = await (req.state.item as Document<any>).save(writeOptions);
        debug.put(req.state.item);
      }
      catch (err) {
        debug.put(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      return await next();
    };

    const afterQuery = async(req: Request, next: Function) => {
      debug.put('afterQueryMiddleWare');
      req.state.resource = { status: 200, item: req.state.item };
      return await next();
    };

    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery,
      after: last,
    };
    this.register('put', `${this.route}/:${this.name}Id`, middlewares, options);
    return this;
  }

  /**
   * Patch (Partial Update) a resource.
   */
  patch(options: any) {
    options = Resource.getMethodOptions('patch', options);
    this.methods.push('patch');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);
    const beforeQuery = async(req: Request, next: Function) => {
      debug.patch('beforeQueryMiddleWare');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'patch';

      if (req.state.skipResource) {
        debug.patch('Skipping Resource');
        return await last(req);
      }
      const id = req.msg.parsed.Subject?.match(`^call\.${this.route}\.(.*)\.${req.state.__rMethod}$`)?.[0]
      if (!id) {
        const err = new Error('ID Not Found')
        debug.put(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      req.state.query = req.state.modelQuery || req.state.model || this.model;
      try {
        req.state.item = await req.state.query.findOne({ '_id': id });
      }
      catch (err) {
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }

      if (!req.state.item) {
        req.state.resource = { status: 404, error: '' };
        return await last(req);
      }

      // Ensure patches is an array
      const patches = [].concat(req.msg.parsed.params);
      let patchFail = null;
      try {
        patches.forEach(async (patch: AddOperation<any> | RemoveOperation | ReplaceOperation<any> | MoveOperation | CopyOperation | TestOperation<any> | GetOperation<any>) => {
          if (patch.op === 'test') {
            patchFail = patch;
            const success = jsonpatch.applyOperation(req.state.item, patch, true);
            if (!success || !success.test) {
              req.state.resource = {
                status: 412,
                name: 'Precondition Failed',
                message: 'A json-patch test op has failed. No changes have been applied to the document',
                item: req.state.item,
                patch,
              };
              return await last(req);
            }
          }
        });
        jsonpatch.applyPatch(req.state.item, patches, true);
      }
      catch (err) {
        switch (err.name) {
          // Check whether JSON PATCH error
          case 'TEST_OPERATION_FAILED':
            req.state.resource = {
              status: 412,
              name: 'Precondition Failed',
              message: 'A json-patch test op has failed. No changes have been applied to the document',
              item: req.state.item,
              patch: patchFail,
            };
            return await last(req);
          case 'SEQUENCE_NOT_AN_ARRAY':
          case 'OPERATION_NOT_AN_OBJECT':
          case 'OPERATION_OP_INVALID':
          case 'OPERATION_PATH_INVALID':
          case 'OPERATION_FROM_REQUIRED':
          case 'OPERATION_VALUE_REQUIRED':
          case 'OPERATION_VALUE_CANNOT_CONTAIN_UNDEFINED':
          case 'OPERATION_PATH_CANNOT_ADD':
          case 'OPERATION_PATH_UNRESOLVABLE':
          case 'OPERATION_FROM_UNRESOLVABLE':
          case 'OPERATION_PATH_ILLEGAL_ARRAY_INDEX':
          case 'OPERATION_VALUE_OUT_OF_BOUNDS':
            err.errors = [{
              name: err.name,
              message: err.toString(),
            }];
            req.state.resource = {
              status: 400,
              item: req.state.item,
              error: err,
            };
            return await last(req);
          // Something else than JSON PATCH
          default:
            req.state.resource = { status: 400, item: req.state.item, error: err };
            return await last(req);
        }
      }
      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.patch('queryMiddleWare');
      const writeOptions = req.state.writeOptions || {};
      try {
        req.state.item = await (req.state.item as Document<any>).save(writeOptions);
      }
      catch (err) {
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      return await next();
    };

    const afterQuery = async(req: Request, next: Function) => {
      debug.patch('afterQueryMiddleWare');
      req.state.resource = { status: 200, item: req.state.item };
      return await next();
    };

    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery,
      after: last,
    };
    this.register('patch', `${this.route}/:${this.name}Id`, middlewares, options);
    return this;
  }

  /**
   * Delete a resource.
   */
  delete(options: any) {
    options = Resource.getMethodOptions('delete', options);
    this.methods.push('delete');
    const last = utils.compose([this.generateMiddleware.call(this, options, 'after'), this.respond]);
    const beforeQuery = async(req: Request, next: Function) => {
      debug.delete('beforeQueryMiddleWare');
      // Store the internal method for response manipulation.
      req.state.__rMethod = 'delete';

      if (req.state.skipResource) {
        debug.delete('Skipping Resource');
        return await last(req);
      }
      const id = req.msg.parsed.Subject?.match(`^call\.${this.route}\.(.*)\.${req.state.__rMethod}$`)?.[0]
      if (!id) {
        const err = new Error('ID Not Found')
        debug.put(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }

      req.state.query = req.state.modelQuery || req.state.model || this.model;

      try {
        req.state.item = await req.state.query.findOne({ '_id': id });
      }
      catch (err) {
        debug.delete(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      if (!req.state.item) {
        debug.delete(`No ${this.name} found with ${this.name}Id: ${id}`);
        req.state.resource = { status: 404, error: '' };
        return await last(req);
      }
      if (req.state.skipDelete) {
        req.state.resource = { status: 204, item: req.state.item, deleted: true };
        return await last(req);
      }
      return await next();
    };

    const query = async(req: Request, next: Function) => {
      debug.delete('queryMiddleWare');
      const writeOptions = req.state.writeOptions || {};
      try {
        req.state.item = await (req.state.item as Document<any>).remove(writeOptions);
      }
      catch (err) {
        debug.delete(err);
        req.state.resource = { status: 400, error: err };
        return await last(req);
      }
      debug.delete(req.state.item);
      return await next();
    };

    const afterQuery = async(req: Request, next: Function) => {
      debug.delete('afterQueryMiddleWare');
      req.state.resource = { status: 204, item: req.state.item, deleted: true };
      return await next();
    };

    const middlewares = {
      before: this.generateMiddleware.call(this, options, 'before'),
      beforeQuery,
      query,
      afterQuery,
      after: last,
    };
    this.register('delete', `${this.route}`, middlewares, options);
    return this;
  }

  /**
   * Returns the swagger definition for this resource.
   */
  swagger(resetCache: any) {
    resetCache = resetCache || false;
    if (!this.__swagger || resetCache) {
      this.__swagger = require('./Swagger')(this);
    }
    return this.__swagger;
  }
}

// Make sure to create a new instance of the Resource class.
function ResourceFactory(app: any, route: String, modelName: String, model: Model<any>, options: any) {
  return new Resource(app, route, modelName, model, options);
}
ResourceFactory.Resource = Resource;

module.exports = ResourceFactory;
