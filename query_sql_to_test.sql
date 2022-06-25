USE mydb_ind_proyect;

CREATE TABLE venta (
	IdVenta INT NOT NULL,
    Fecha VARCHAR(20),
    Fecha_Entrega VARCHAR(20),
    IdCanal INT NOT NULL,
    IdCliente INT NOT NULL,
    IdSucursal INT NOT NULL,
    IdEmpleado INT NOT NULL,
    IdProducto INT NOT NULL,
    Precio DECIMAL(10,2),
    Cantidad INT NOT NULL
    ) ;

CREATE TABLE cliente(
	IdCliente INT NOT NULL,
	Provincia	VARCHAR(120),
    Nombre_completo	VARCHAR(120),
    Domicilio	VARCHAR(120),
    Telefono	VARCHAR(120),
    Edad	INT NOT NULL,
    Localidad	VARCHAR(120),
    Longitud	DECIMAL(8,6),
    Latitud	DECIMAL(8,6)
);

CREATE TABLE compra(
IdCompra	INT NOT NULL,
Fecha	VARCHAR(120),
Fecha_Año	INT NOT NULL,
Fecha_Mes	INT NOT NULL,
Fecha_Periodo	INT NOT NULL,
IdProducto	INT NOT NULL,
Cantidad	INT NOT NULL,
Precio	INT NOT NULL,
IdProveedor INT NOT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE gasto(
	IdGasto	INT NOT NULL,
	IdSucursal	INT NOT NULL,
	IdTipoGasto	INT NOT NULL,
	Fecha	VARCHAR(50),
	Monto INT NOT NULL
);

CREATE TABLE sucursal(
	IdSucursal	INT NOT NULL,
    Sucursal VARCHAR(120),
    Direccion VARCHAR(120),
    Localidad VARCHAR(120),
    Provincia VARCHAR(120),
    Latitud	 DECIMAL(10,8),
    Longitud DECIMAL(10,8)
);

CREATE TABLE proveedor(
	IdProveedor	INT NOT NULL,
    Nombre	VARCHAR(30),
    Domicilio	VARCHAR(30),
    Municipio	VARCHAR(30),
    Provincia	VARCHAR(30),
    Departamento VARCHAR(30)
);


SHOW TABLES;
 DROP TABLE cliente;
 DROP TABLE venta;
 DROP TABLE gasto;
 DROP TABLE compra;
 DROP TABLE sucursal;
 DROP TABLE proveedor;
 
SELECT * FROM cliente LIMIT 10;
SELECT DISTINCT Provincia FROM cliente;
SELECT * FROM compra LIMIT 10;
SELECT * FROM venta LIMIT 10;
SELECT * FROM sucursal LIMIT 10;
SELECT * FROM proveedor;
SELECT * FROM gasto;
SELECT COUNT(*) from venta;

