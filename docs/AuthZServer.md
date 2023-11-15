# AuthZServer REST Protocols for DeltaStore

## ListShares

DeltaStore requests checking if a token is allowed to list shares

### Requests

DeltaStore requests a POST method as below.

```
POST /list-shares
```

DeltaStore must include a JSON formatted body of a HTTP request as below.

```
{
    token: string
}
```

### Responses

AuthZServer must include a JSON formatted body of a HTTP response as below.

```
{
    success: boolean,   // true if a action is allowed
    reason: string,     // reason message if a action is denied
}
```

## ListSchemas

DeltaStore requests checking if a token is allowed to list schemas in a share

### Requests

DeltaStore requests a POST method as below.

```
POST /list-schemas
```

DeltaStore must include a JSON formatted body of a HTTP request as below.

```
{
    token: string,
    share: string
}
```

### Responses

AuthZServer must include a JSON formatted body of a HTTP response as below.

```
{
    success: boolean,   // true if a action is allowed
    reason: string,     // reason message if a action is denied
}
```

## ListTables

DeltaStore requests checking if a token is allowed to list tables in a share/schema

### Requests

DeltaStore requests a POST method as below.

```
POST /list-tables
```

DeltaStore must include a JSON formatted body of a HTTP request as below.

```
{
    token: string,
    share: string,
    schema: string
}
```

### Responses

AuthZServer must include a JSON formatted body of a HTTP response as below.

```
{
    success: boolean,   // true if a action is allowed
    reason: string,     // reason message if a action is denied
}
```

## ListAllTables

DeltaStore requests checking if a token is allowed to list tables in a share

### Requests

DeltaStore requests a POST method as below.

```
POST /list-all-tables
```

DeltaStore must include a JSON formatted body of a HTTP request as below.

```
{
    token: string,
    share: string
}
```

### Responses

AuthZServer must include a JSON formatted body of a HTTP response as below.

```
{
    success: boolean,   // true if a action is allowed
    reason: string,     // reason message if a action is denied
}
```

## ListFiles

DeltaStore requests checking if a token is allowed to list files on a table in a share/schema.

### Requests

DeltaStore requests a POST method as below.

```
POST /list-files
```

DeltaStore must include a JSON formatted body of a HTTP request as below.

```
{
    token: string,
    share: string,
    schema: string,
    table: string
}
```

### Responses

AuthZServer must include a JSON formatted body of a HTTP response as below.

```
{
    success: boolean,   // true if a action is allowed
    reason: string,     // reason message if a action is denied
    filters: [string]   // partition filters which can be used for filtering accessable partitions
}
```

For example, if you want to allow only partitions for March 15, 2023, filters can be as below.

```
{
    filters: ['date="2023-03-15"']
}
```

And, if you want to allow partitions since 2022, filters can be as below.

```
{
    filters: ['date>="2022-01-01"']
}
```
