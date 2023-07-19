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
