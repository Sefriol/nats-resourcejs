import { ParsedQs } from "qs";
import { MethodOptions } from './interfaces'
import { ObjectId } from 'mongoose';
import moment from 'moment';

const zipObject = (props: any[], values: string[]) => props.reduce((prev: Object, prop: string, i: string | number) => Object.assign(prev, { [prop]: values[i] }), {});

const isObjectLike = (obj: any) => obj !== null && typeof obj === 'object';

const get = (obj: any, path: string, defaultValue: any = null) => path.split('.').reduce((a: any, c: string) => (a && a[c] ? a[c] : defaultValue), obj);

const set = (obj: object, path: string | Array<string>, value: any) => {
    if (Object(obj) !== obj) return obj;
    // If not yet an array, get the keys from the string-path
    if (!Array.isArray(path)) path = path.toString().match(/[^.[\]]+/g) || [];
    // Split the path. Note: last index is the value key
    path.slice(0,-1).reduce((a:any, c:string, i:number) =>
         Object(a[c]) === a[c] // Does the key exist and is its value an object?
             // Yes: then follow that path
             ? a[c]
             // No: create the key. Is the next key a potential array-index?
             : a[c] = Math.abs(path[i+1])>>0 === +path[i+1]
                   ? [] // Yes: assign a new array object
                   : {}, // No: assign a new plain object
         obj)[path[path.length-1]] = value; // Finally assign the value to the last key
    return obj;
};

const isEmpty = (obj: object) => {
  return !obj || (Object.entries(obj).length === 0 && obj.constructor === Object);
};

/**
 * Fork of https://github.com/polo2ro/node-paginate-anything
 * Modify the http response for pagination, return 2 properties to use in a query
 *
 * @url https://github.com/begriffs/clean_pagination
 * @url http://nodejs.org/api/http.html#http_class_http_clientrequest
 * @url http://nodejs.org/api/http.html#http_class_http_serverresponse
 *
 *
 * @param	{Koa.Context}	        ctx				      A Koa Context encapsulates node's request and response objects into a single object
 * @param	{Number}					        totalItems 	    total number of items available, can be Infinity
 * @param	{Number}					        maxRangeSize
 *
 * @return {Object}
 * 			.limit	Number of items to return
 * 			.skip	Zero based position for the first item to return
 */

// eslint-disable-next-line max-statements
const paginate = function (ctx: any, totalItems: number, maxRangeSize: number): {limit:number, skip: number} | undefined {
	/**
	 * Parse requested range
	 */
  function parseRange(hdr: String) {
    var m = hdr && hdr.match(/^(\d+)-(\d*)$/);
    if (!m) {
      return null;
    }
    return {
      from: parseInt(m[1]),
      to: m[2] ? parseInt(m[2]) : Infinity,
    };
  }

  ctx.set('Accept-Ranges', 'items');
  ctx.set('Range-Unit', 'items');
  ctx.set('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Range-Unit');

  maxRangeSize = parseInt(maxRangeSize);

  var range = {
    from: 0,
    to: (totalItems - 1),
  };

  if ('items' === ctx.headers['range-unit']) {
    var parsedRange = parseRange(ctx.headers.range);
    if (parsedRange) {
      range = parsedRange;
    }
  }

  if ((null !== range.to && range.from > range.to) || (range.from > 0 && range.from >= totalItems)) {
    if (totalItems > 0 || range.from !== 0) {
      ctx.status = 416; // Requested range unsatisfiable
    }
    else {
      ctx.status = 204; // No content
    }
    ctx.set('Content-Range', `*/${totalItems}`);
    return;
  }

  var availableTo;
  var reportTotal;

  if (totalItems < Infinity) {
    availableTo = Math.min(
      range.to,
      totalItems - 1,
      range.from + maxRangeSize - 1
    );

    reportTotal = totalItems;
  }
  else {
    availableTo = Math.min(
      range.to,
      range.from + maxRangeSize - 1
    );

    reportTotal = '*';
  }

  ctx.set('Content-Range', `${range.from}-${availableTo}/${reportTotal}`);

  var availableLimit = availableTo - range.from + 1;

  if (0 === availableLimit) {
    ctx.status = 204; // no content
    ctx.set('Content-Range', '*/0');
    return;
  }

  if (availableLimit < totalItems) {
    ctx.status = 206; // Partial contents
  }
  else {
    ctx.status = 200; // OK (all items)
  }

  // Links
  function buildLink(rel: string, itemsFrom: number, itemsTo: number) {
    var to = itemsTo < Infinity ? itemsTo : '';
    return `<${ctx.url}>; rel="${rel}"; items="${itemsFrom}-${to}"`;
  }

  var requestedLimit = range.to - range.from + 1;
  var links = [];

  if (availableTo < totalItems - 1) {
    links.push(buildLink('next',
      availableTo + 1,
      availableTo + requestedLimit
    ));

    if (totalItems < Infinity) {
      var lastStart = Math.floor((totalItems - 1) / availableLimit) * availableLimit;

      links.push(buildLink('last',
        lastStart,
        lastStart + requestedLimit - 1
      ));
    }
  }

  if (range.from > 0) {
    var previousFrom = Math.max(0, range.from - Math.min(requestedLimit, maxRangeSize));
    links.push(buildLink('prev',
      previousFrom,
      previousFrom + requestedLimit - 1
    ));

    links.push(buildLink('first',
      0,
      requestedLimit - 1
    ));
  }

  ctx.set('Link', links.join(', '));

  // return values named from mongoose methods
  return {
    limit: availableLimit,
    skip: range.from,
  };
};

/**
 * 
 * @param {Array<Function>} middleware array of middleware functions
 * @returns {Function} function where next() goes to next middleware in the array.
 */

function compose(middleware: Array<Function>): Function {
  if (!Array.isArray(middleware)) throw new TypeError('Middleware stack must be an array!')
  for (const fn of middleware) {
    if (typeof fn !== 'function') throw new TypeError('Middleware must be composed of functions!')
  }

  /**
   * @param {Object} context request context
   * @param {Function} next
   * @return {Promise}
   * @api public
   */

  return function (context: object, next: Function): Promise<any> {
    // last called middleware #
    let index = -1
    return dispatch(0)
    function dispatch(i: number): Promise<any> {
      if (i <= index) return Promise.reject(new Error('next() called multiple times'))
      index = i
      let fn = middleware[i]
      if (i === middleware.length) fn = next
      if (!fn) return Promise.resolve()
      try {
        return Promise.resolve(fn(context, dispatch.bind(null, i + 1)));
      } catch (err) {
        return Promise.reject(err)
      }
    }
  }
}

/**
   * Returns a query parameters fields.
   *
   * @param { [key: string]: undefined | string | string[]} requestQuery
   * @param {string} name
   * @returns {*}
   */
const getParamQuery = (requestQuery: ParsedQs | undefined, name: string): any => {
  if (!Object.prototype.hasOwnProperty.call(requestQuery, name)) {
    switch (name) {
      case 'populate':
        return '';
      default:
        return null;
    }
  }
  let query = requestQuery?[name]
  if (name === 'populate' && isObjectLike(query)) {
    return query;
  }
  else {
    query = Array.isArray(query) ? query!.join(',') : query;
    // Generate string of spaced unique keys
    return (query && typeof query === 'string') ? [...new Set(query.match(/[^, ]+/g))].join(' ') : null;
  }
}

const getQueryValue = (name: string, value: string | string[] | number | boolean | ObjectId | Date | null | undefined, param:any, options: MethodOptions, selector: string) => {
  if (selector && (selector === 'eq' || selector === 'ne') && (typeof value === 'string')) {
    const lcValue = value.toLowerCase();
    switch (lcValue) {
      case 'null':
        return null;
      case '"null"':
        return 'null';
      case 'true':
        return true;
      case '"true"':
        return 'true';
      case 'false':
        return false;
      case '"false"':
        return 'false';
      default:
        break;
    }
  }

  if (param.instance === 'Number') {
    // @ts-ignore value is string if param instance 
    return parseInt(value, 10);
  }

  if (param.instance === 'Date') {
    // @ts-ignore
    const date = moment.utc(value, ['YYYY-MM-DD', 'YYYY-MM', 'YYYY', 'x', moment.ISO_8601], true);
    if (date.isValid()) {
      return date.toDate();
    }
  }

  // If this is an ID, and the value is a string, convert to an ObjectId.
  if (
    options.convertIds &&
    name.match(options.convertIds) &&
    (typeof value === 'string') &&
    (ObjectId.isValid(value))
  ) {
    try {
      value = new ObjectId(value);
    }
    catch (err) {
      console.warn(`Invalid ObjectID: ${value}`);
    }
  }

  return value;
}

export { getParamQuery, getQueryValue, zipObject, isObjectLike, isEmpty, get, set, paginate, compose };
