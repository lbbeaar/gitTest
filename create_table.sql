CREATE TABLE [dbo].[Laptop] (
	[code] [int] NOT NULL ,
	[model] [varchar] (50) NOT NULL ,
	[speed] [smallint] NOT NULL ,
	[ram] [smallint] NOT NULL ,
	[hd] [real] NOT NULL ,
	[price] [money] NULL ,
	[screen] [tinyint] NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[PC] (
	[code] [int] NOT NULL ,
	[model] [varchar] (50) NOT NULL ,
	[speed] [smallint] NOT NULL ,
	[ram] [smallint] NOT NULL ,
	[hd] [real] NOT NULL ,
	[cd] [varchar] (10) NOT NULL ,
	[price] [money] NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Product] (
	[maker] [varchar] (10) NOT NULL ,
	[model] [varchar] (50) NOT NULL ,
	[type] [varchar] (50) NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Printer] (
	[code] [int] NOT NULL ,
	[model] [varchar] (50) NOT NULL ,
	[color] [char] (1) NOT NULL ,
	[type] [varchar] (10) NOT NULL ,
	[price] [money] NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Laptop] WITH NOCHECK ADD
	CONSTRAINT [PK_Laptop] PRIMARY KEY
	(
		[code]
	)  ON [PRIMARY]
GO

ALTER TABLE [dbo].[PC] WITH NOCHECK ADD
	CONSTRAINT [PK_pc] PRIMARY KEY
	(
		[code]
	)  ON [PRIMARY]
GO

ALTER TABLE [dbo].[Product] WITH NOCHECK ADD
	CONSTRAINT [PK_product] PRIMARY KEY
	(
		[model]
	)  ON [PRIMARY]
GO

ALTER TABLE [dbo].[Printer] WITH NOCHECK ADD
	CONSTRAINT [PK_printer] PRIMARY KEY
	(
		[code]
	)  ON [PRIMARY]
GO

ALTER TABLE [dbo].[Laptop] ADD
	CONSTRAINT [FK_Laptop_product] FOREIGN KEY
	(
		[model]
	) REFERENCES [dbo].[Product] (
		[model]
	)
GO

ALTER TABLE [dbo].[PC] ADD
	CONSTRAINT [FK_pc_product] FOREIGN KEY
	(
		[model]
	) REFERENCES [dbo].[Product] (
		[model]
	)
GO

ALTER TABLE [dbo].[Printer] ADD
	CONSTRAINT [FK_printer_product] FOREIGN KEY
	(
		[model]
	) REFERENCES [dbo].[Product] (
		[model]
	)
GO