## 0.1.0 - 2014-11-13
Beta release of spatial APIs


## 1.0.0 - 2015-10-30

### Changed
- re-implement `create_geometry`
- stop `delete` APIs actually deleting peoples application Nodes and only remove the relationship to the spatial index
- refactor APIs to respect GIS standards
- breaking signature changes to APIs for consistency going forward
- re-write of documentation
- return upstream API HTTP responses instead of Node instances on CRUD operations

### Added
- API to find interesting geometries from a given location
- API to find the geometries that contain a POI
- increase in test coverage

### Fixed
- no longer match on WKT strings to avoid rounding precision bugs
- geometry names only need be unique across a single Layer
