# ConfigServer REST Protocols for DeltaStore

For all list requests, if ConfigServer have too many items, it should support pagination mechanism using a token. ConfigServer should include optionally a token in a JSON formatted body of a HTTP response as below.

```
{
    ...,
    token: string (optional)
}
```

And then, DeltaStore will send a next request with the token parameter as below.

```
GET /...?token={nextToken}
```

## ListShares

DeltaStore requests all available shares.

### Requests

```
GET /shares?token={nextToken}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    items: [
        {
            name: string
        },
        ...
    ],
    token: string (optional)
}
```

## ListSchemas

DeltaStore requests all available schemas in a share.

### Requests

```
GET /shares/{shareName}/schemas?token={nextToken}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    items: [
        {
            name: string,
            share: string
        },
        ...
    ],
    token: string (optional)
}
```

## ListTables

DeltaStore requests all available tables in a share and a schema.

### Requests

```
GET /shares/{shareName}/schemas/{schemaName}/tables?token={nextToken}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    items: [
        {
            name: string,
            share: string,
            schema: string
        },
        ...
    ],
    token: string (optional)
}
```

## ListAllTables

DeltaStore requests all available tables in a share.

### Requests

```
GET /shares/{shareName}/tables?token={nextToken}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    items: [
        {
            name: string,
            share: string,
            schema: string
        },
        ...
    ],
    token: string (optional)
}
```

## GetFilesystem

DeltaStore requests a filesystem config.

### Requests

```
GET /filesystems/{filesystemName}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    name: string,
    configs: [
        {
            key: string,
            value: string
        }
    ]
}
```

## GetTable

DeltaStore requests a table config.

### Requests

```
GET /shares/{shareName}/schemas/{schemaName}/tables/{tableName}
```

### Responses

ConfigServer must include a JSON formatted body of a HTTP response as below.

```
{
    name: string,
    share: string,
    schema: string,
    repository: string,   // a repository name which is used for saving this table's log caches
    filesystem: string,   // a filesystem name which contains this table
    location: string      // a location in a filesystem which contains this table
    versioning: boolean   // true if versioning support is enabled
}
```
