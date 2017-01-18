# appmonit-db

This shard will contain the code used for the internal storage layer for Appmonit

## Installation

Add this to your application's `shard.yml`:

```yaml
dependencies:
  appmonit-db:
    github: appmonit/appmonit-db
```

## Usage

```crystal
require "appmonit-db"
```


## File conventions

* **Collection**: /some-dir/collection-name
* **Shard**:      /some-dir/collection-name/[start-epoch]-[end-epoch]
* **ADB File**:   /some-dir/collection-name/[start-epoch]-[end-epoch]/[level]-[sequence].adb
* **WAL Files**:  /some-dir/collection-name/_0000000000[sequence].wal


## Development

TODO: Write development instructions here

## Contributing

1. Fork it ( https://github.com/[your-github-name]/appmonit-db/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [benoist](https://github.com/benoist]) Benoist Claassen - creator, maintainer
