USE [master]
GO

ALTER LOGIN [sa] ENABLE
GO
ALTER LOGIN [sa] WITH PASSWORD=N'123'
GO

USE [vl190384]
GO

DROP TABLE IF EXISTS [Transaction]
go

DROP TABLE IF EXISTS [OrderHasArticle]
go

DROP TABLE IF EXISTS [Order]
go

DROP TABLE IF EXISTS [Buyer]
go

DROP TABLE IF EXISTS [Article]
go

DROP TABLE IF EXISTS [Shop]
go

DROP TABLE IF EXISTS [Connection]
go

DROP TABLE IF EXISTS [City]
go

DROP TYPE IF EXISTS [OrderStateDomain]
go


CREATE TYPE [OrderStateDomain]
	FROM CHAR(100) NOT NULL
go

CREATE TABLE [Article]
( 
	[IdArticle]          integer  IDENTITY  NOT NULL ,
	[Name]               char(100)  NOT NULL ,
	[Price]              integer  NOT NULL ,
	[Count]              integer  NOT NULL 
	CONSTRAINT [DefaultZero_583821167]
		 DEFAULT  0,
	[IdShop]             integer  NOT NULL 
)
go

CREATE TABLE [Buyer]
( 
	[IdBuyer]            integer  IDENTITY  NOT NULL ,
	[Name]               char(100)  NOT NULL ,
	[Credit]             decimal(10,3)  NOT NULL 
	CONSTRAINT [DefaultZero_468344172]
		 DEFAULT  0,
	[IdCity]             integer  NOT NULL 
)
go

CREATE TABLE [City]
( 
	[IdCity]             integer  IDENTITY  NOT NULL ,
	[Name]               char(100)  NOT NULL 
)
go

CREATE TABLE [Connection]
( 
	[Distance]           integer  NOT NULL 
	CONSTRAINT [GreaterOrEqualToZero_1923022623]
		CHECK  ( Distance >= 0 ),
	[IdCity1]            integer  NOT NULL ,
	[IdCity2]            integer  NOT NULL ,
	[IdConnection]       integer  IDENTITY NOT NULL 
)
go

CREATE TABLE [Order]
( 
	[IdOrder]            integer  IDENTITY  NOT NULL ,
	[Status]             [OrderStateDomain] 
	CONSTRAINT [DefaultOrderState_522394709]
		 DEFAULT  'created',
	[SentTime]           datetime  NULL ,
	[ReceivedTime]       datetime  NULL ,
	[FinalPrice]         decimal(10,3)  NULL ,
	[DiscountSum]        decimal(10,3)  NULL ,
	[Location]           integer  NULL ,
	[IdBuyer]            integer  NOT NULL 
)
go

CREATE TABLE [OrderHasArticle]
( 
	[Count]              integer  NULL ,
	[IdOrderHasArticle]  integer  IDENTITY  NOT NULL ,
	[IdArticle]          integer  NOT NULL ,
	[IdOrder]            integer  NOT NULL 
)
go

CREATE TABLE [Shop]
( 
	[IdShop]             integer  IDENTITY  NOT NULL ,
	[Name]               char(100)  NOT NULL ,
	[Discount]           integer  NOT NULL 
	CONSTRAINT [DefaultZero_69634971]
		 DEFAULT  0
	CONSTRAINT [BetweenZeroAndHundred_1298019925]
		CHECK  ( Discount BETWEEN 0 AND 100 ),
	[IdCity]             integer  NOT NULL 
)
go

CREATE TABLE [Transaction]
( 
	[Amount]             decimal(10,3)  NOT NULL ,
	[TimeOfExecution]    datetime  NULL ,
	[IdTransaction]      integer  IDENTITY  NOT NULL ,
	[IdOrder]            integer  NOT NULL ,
	[IdBuyer]            integer  NULL ,
	[IdShop]             integer  NULL 
)
go

ALTER TABLE [Article]
	ADD CONSTRAINT [XPKArticle] PRIMARY KEY  CLUSTERED ([IdArticle] ASC)
go

ALTER TABLE [Buyer]
	ADD CONSTRAINT [XPKBuyer] PRIMARY KEY  CLUSTERED ([IdBuyer] ASC)
go

ALTER TABLE [City]
	ADD CONSTRAINT [XPKCity] PRIMARY KEY  CLUSTERED ([IdCity] ASC)
go

ALTER TABLE [City]
	ADD CONSTRAINT [XAK1City] UNIQUE ([Name]  ASC)
go

ALTER TABLE [Connection]
	ADD CONSTRAINT [XPKConnection] PRIMARY KEY  CLUSTERED ([IdConnection] ASC)
go

ALTER TABLE [Connection]
	ADD CONSTRAINT [XAK1Connection] UNIQUE ([IdCity1]  ASC,[IdCity2]  ASC)
go

ALTER TABLE [Order]
	ADD CONSTRAINT [XPKOrder] PRIMARY KEY  CLUSTERED ([IdOrder] ASC)
go

ALTER TABLE [OrderHasArticle]
	ADD CONSTRAINT [XPKOrderHasArticle] PRIMARY KEY  CLUSTERED ([IdOrderHasArticle] ASC)
go

ALTER TABLE [OrderHasArticle]
	ADD CONSTRAINT [XAK1OrderHasArticle] UNIQUE ([IdOrder]  ASC,[IdArticle]  ASC)
go

ALTER TABLE [Shop]
	ADD CONSTRAINT [XPKShop] PRIMARY KEY  CLUSTERED ([IdShop] ASC)
go

ALTER TABLE [Shop]
	ADD CONSTRAINT [XAK1Shop] UNIQUE ([Name]  ASC)
go

ALTER TABLE [Transaction]
	ADD CONSTRAINT [XPKTransaction] PRIMARY KEY  CLUSTERED ([IdTransaction] ASC)
go


ALTER TABLE [Article]
	ADD CONSTRAINT [ShopArticle] FOREIGN KEY ([IdShop]) REFERENCES [Shop]([IdShop])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go


ALTER TABLE [Buyer]
	ADD CONSTRAINT [CityBuyer] FOREIGN KEY ([IdCity]) REFERENCES [City]([IdCity])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go


ALTER TABLE [Connection]
	ADD CONSTRAINT [CityConnection1] FOREIGN KEY ([IdCity1]) REFERENCES [City]([IdCity])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go

ALTER TABLE [Connection]
	ADD CONSTRAINT [CityConnection2] FOREIGN KEY ([IdCity2]) REFERENCES [City]([IdCity])
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go


ALTER TABLE [Order]
	ADD CONSTRAINT [CityOrder] FOREIGN KEY ([Location]) REFERENCES [City]([IdCity])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go

ALTER TABLE [Order]
	ADD CONSTRAINT [BuyerOrder] FOREIGN KEY ([IdBuyer]) REFERENCES [Buyer]([IdBuyer])
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go


ALTER TABLE [OrderHasArticle]
	ADD CONSTRAINT [ArticleOrderHasArticle] FOREIGN KEY ([IdArticle]) REFERENCES [Article]([IdArticle])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go

ALTER TABLE [OrderHasArticle]
	ADD CONSTRAINT [OrderOrderHasArticle] FOREIGN KEY ([IdOrder]) REFERENCES [Order]([IdOrder])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go


ALTER TABLE [Shop]
	ADD CONSTRAINT [CityShop] FOREIGN KEY ([IdCity]) REFERENCES [City]([IdCity])
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go


ALTER TABLE [Transaction]
	ADD CONSTRAINT [OrderTransaction] FOREIGN KEY ([IdOrder]) REFERENCES [Order]([IdOrder])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go

ALTER TABLE [Transaction]
	ADD CONSTRAINT [BuyerTransaction] FOREIGN KEY ([IdBuyer]) REFERENCES [Buyer]([IdBuyer])
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
go

ALTER TABLE [Transaction]
	ADD CONSTRAINT [ShopTransaction] FOREIGN KEY ([IdShop]) REFERENCES [Shop]([IdShop])
		ON DELETE NO ACTION
		ON UPDATE CASCADE
go

USE [vl190384]
GO 
DROP FUNCTION IF EXISTS F_CHECK_FOR_EXTRA_DISCOUNT
GO

CREATE FUNCTION F_CHECK_FOR_EXTRA_DISCOUNT
(
	@curDate datetime,
	@orderId int
)
RETURNS int
AS
BEGIN
	declare @paidSum decimal(10,3)

	SELECT @paidSum=SUM([FinalPrice]) FROM [dbo].[Order]
	WHERE [IdBuyer] = (SELECT [IdBuyer] FROM [dbo].[Order] WHERE [IdOrder] = @orderId) AND
	DATEDIFF(day, [SentTime], @curDate) <= 30

	RETURN CASE WHEN @paidSum > 10000 THEN 1 ELSE 0 END
END
GO

DROP PROCEDURE IF EXISTS SP_FINAL_PRICE
GO

CREATE PROCEDURE SP_FINAL_PRICE
	@orderId int,
	@discountGranted int
AS
BEGIN
	declare @totalSum decimal(10,3), 
		@discountSum decimal(10,3)

	SELECT @totalSum = SUM(oha.[Count] * a.[Price]) 
	FROM [dbo].[OrderHasArticle] oha 
	join [dbo].[Article] a on a.[IdArticle] = oha.[IdArticle]
	WHERE oha.IdOrder = @orderId

	SELECT @discountSum = SUM(oha.[Count] * a.[Price] * s.[Discount]/100.0) 
	FROM [dbo].[OrderHasArticle] oha 
	join [dbo].[Article] a on a.[IdArticle] = oha.[IdArticle] 
	join [dbo].[Shop] s on s.[IdShop] = a.[IdShop]
	WHERE oha.IdOrder = @orderId

	SET @totalSum = @totalSum - @discountSum
	IF @discountGranted = 1
	BEGIN
		SET @discountSum = @discountSum + @totalSum*0.02
		SET @totalSum = @totalSum * 0.98
	END

	UPDATE [dbo].[Order]
	   SET [FinalPrice] = @totalSum,
		   [DiscountSum] = @discountSum
	 WHERE [IdOrder] = @orderId

END
GO

DROP PROCEDURE IF EXISTS SP_GENERATE_TRANSACTIONS
GO

CREATE PROCEDURE SP_GENERATE_TRANSACTIONS
	@curDate datetime,
	@orderId int
AS
BEGIN
	declare @shopId int, @amount decimal(10,3)

	DECLARE shopCursor CURSOR FOR   
    SELECT s.[IdShop], SUM(oha.[Count] * a.[Price] * (1 - s.[Discount]/100.0)) as Amount 
	FROM [dbo].[OrderHasArticle] oha 
	join [dbo].[Article] a on a.[IdArticle] = oha.[IdArticle] 
	join [dbo].[Shop] s on s.[IdShop] = a.[IdShop]
	WHERE oha.IdOrder = @orderId
	GROUP BY s.[IdShop]
  
    OPEN shopCursor  
    FETCH NEXT FROM shopCursor INTO @shopId, @amount  
 
    WHILE @@FETCH_STATUS = 0  
    BEGIN  
		
		INSERT INTO [dbo].[Transaction]
				   ([Amount],
					[IdOrder],
					[IdShop])
			 VALUES
				   (@amount*0.95,
					@orderId,
					@shopId)
		
		FETCH NEXT FROM shopCursor INTO @shopId, @amount  
	END  
  
    CLOSE shopCursor  
    DEALLOCATE shopCursor 

	INSERT INTO [dbo].[Transaction]
				   ([Amount],
				    [TimeOfExecution],
					[IdOrder],
					[IdBuyer])
			 VALUES
				   ((SELECT [FinalPrice] FROM [dbo].[Order] WHERE [IdOrder]=@orderId),
					@curDate,
					@orderId,
					(SELECT [IdBuyer] FROM [dbo].[Order] WHERE [IdOrder]=@orderId))
END
GO

DROP TRIGGER IF EXISTS TR_TRANSFER_MONEY_TO_SHOPS
GO

CREATE TRIGGER TR_TRANSFER_MONEY_TO_SHOPS 
   ON  [dbo].[Order]
   FOR UPDATE
   AS 
BEGIN
	declare @orderId int, @date datetime

	DECLARE myCursor CURSOR FOR   
    SELECT [IdOrder], [ReceivedTime]
	FROM inserted WHERE [Status] = 'arrived'
  
    OPEN myCursor  
    FETCH NEXT FROM myCursor INTO @orderId, @date
 
    WHILE @@FETCH_STATUS = 0  
    BEGIN  
		
		UPDATE [dbo].[Transaction]
		SET [TimeOfExecution] = @date
		WHERE [IdOrder] = @orderId AND [IdBuyer] IS NULL
		
		FETCH NEXT FROM myCursor INTO @orderId, @date
	END  
  
    CLOSE myCursor  
    DEALLOCATE myCursor 

END
GO
