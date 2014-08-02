


############################################################################################################
# njs_util                  = require 'util'
# njs_path                  = require 'path'
# njs_fs                    = require 'fs'
#...........................................................................................................
# BAP                       = require 'coffeenode-bitsnpieces'
TYPES                     = require 'coffeenode-types'
# TEXT                      = require 'coffeenode-text'
TRM                       = require 'coffeenode-trm'
rpr                       = TRM.rpr.bind TRM
badge                     = 'n4j'
log                       = TRM.get_logger 'plain',     badge
info                      = TRM.get_logger 'info',      badge
whisper                   = TRM.get_logger 'whisper',   badge
alert                     = TRM.get_logger 'alert',     badge
debug                     = TRM.get_logger 'debug',     badge
warn                      = TRM.get_logger 'warn',      badge
help                      = TRM.get_logger 'help',      badge
urge                      = TRM.get_logger 'urge',      badge
echo                      = TRM.echo.bind TRM
rainbow                   = TRM.rainbow.bind TRM
#...........................................................................................................
ASYNC                     = require 'async'
mk_request                = require 'request'
#...........................................................................................................
### TAINT should use proper options ###
db_route                  = 'http://localhost:7474/db/data/cypher'


#-----------------------------------------------------------------------------------------------------------
@_request = ( query, handler ) ->
  http_options  =
    url:      db_route
    method:   'POST'
    json:     yes
    body:     query
  #---------------------------------------------------------------------------------------------------------
  rq = mk_request http_options, ( error, response, body ) =>
    return handler error if error?
    { exception, message, } = body
    if exception?
      # warn body
      return handler new Error exception + ': ' + message
    { data, columns  } = body
    if data?
      for row in data
        for entry, entry_idx in row
          continue unless TYPES.isa_pod entry
          continue unless ( { self } = entry )?
          kernel = entry[ 'data' ]
          continue unless TYPES.isa_pod kernel
          if isa_node = /\/node\/[0-9]+$/.test self
            kernel[ '~isa' ] = 'node'
          else
            kernel[ '~isa' ] = 'edge'
          row[ entry_idx ] = kernel
    handler null, data, columns
  # rq.on 'data', ( chunk ) =>
  #   whisper chunk.length
  #---------------------------------------------------------------------------------------------------------
  return null



#-----------------------------------------------------------------------------------------------------------
@clear_db = ( handler ) ->
  query = query: """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]-()
    DELETE n, r;"""
  # query = query: """MATCH (n) RETURN n"""
  # query = query: """MATCH (n)-[r]-(m) RETURN n, r, m"""
  # query = query: """MATCH (n)-[r]-(m) RETURN r, LABELS( r )"""
  #---------------------------------------------------------------------------------------------------------
  @_request query, ( error, data, columns ) =>
    return handler error if error?
    handler null
    # debug data
    # info columns
  #---------------------------------------------------------------------------------------------------------
  return null


#-----------------------------------------------------------------------------------------------------------
@read_labels = ( handler ) ->
  query = query: """MATCH n RETURN DISTINCT LABELS( n )"""
  #.........................................................................................................
  @_request query, ( error, rows ) =>
    return handler error if error?
    Z = {}
    for row in rows
      for label in row
        Z[ label ] = 1
    handler null, ( label for label of Z )
  #.........................................................................................................
  return null

#-----------------------------------------------------------------------------------------------------------
@read_names_by_label = ( handler ) ->
  ### Yields a POD whose keys are labels and whose values are lists of property names found for objects
  with that label. Note that results are only reliable for collections where all elements have exactly
  one label and all objects with the same label have the exact same property names defined. ###
  #.........................................................................................................
  @read_labels ( error, labels ) =>
    throw error if error?
    info labels
    tasks = []
    #.......................................................................................................
    for label in labels
      do ( label ) =>
        #...................................................................................................
        tasks.push ( async_handler ) =>
          query = query: """MATCH (n:#{@_escape_name label}) RETURN n LIMIT 1"""
          @_request query, ( error, rows ) =>
            async_handler error, rows[ 0 ][ 0 ]
    #.......................................................................................................
    on_finish = ( error, nodes ) =>
      throw error if error?
      whisper nodes
      Z = {}
      for node in nodes
        target = Z[ node[ '~label' ] ] = []
        for name of node
          target.push name
      urge Z
      handler null, Z
    #.......................................................................................................
    ASYNC.parallelLimit tasks, 10, on_finish
  #.........................................................................................................
  return null



#-----------------------------------------------------------------------------------------------------------
@_escape = ( x ) ->
  return switch type = TYPES.type_of x
    when 'pod'      then @_escape_pod  x
    when 'node'     then @_escape_node x
    else JSON.stringify x
    # else throw new Error "unable to escape value of type #{rpr type}"

#-----------------------------------------------------------------------------------------------------------
@_escape_name = ( x ) ->
  return '`' + ( x.replace /`/g, '``' ) + '`'

#-----------------------------------------------------------------------------------------------------------
@_escape_pod = ( x ) ->
  R = ( ( @_escape_name name ) + ': ' + @_escape value for name, value of x )
  return "{ #{R.join ', '} }"

#-----------------------------------------------------------------------------------------------------------
@_escape_node = ( x ) ->
  label = @_escape_name x[ '~label' ] ? 'nolabel'
  data  = @_escape_pod  x
  return "(:#{label} #{data})"
