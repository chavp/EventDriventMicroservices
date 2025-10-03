using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderManagement.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrders : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "orders");

            migrationBuilder.CreateTable(
                name: "InsuredAssets",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Description = table.Column<string>(type: "character varying(3000)", maxLength: 3000, nullable: false),
                    BookValue = table.Column<decimal>(type: "numeric", nullable: false),
                    Parties_AssetId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_InsuredAssets", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "OrderRoleTypes",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    Name = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderRoleTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Orders",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderNumber = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    OrderDate = table.Column<DateOnly>(type: "date", nullable: true),
                    TotalAmount = table.Column<decimal>(type: "numeric", nullable: false),
                    TotalQuantity = table.Column<int>(type: "integer", nullable: false),
                    Products_TenantId = table.Column<Guid>(type: "uuid", nullable: true),
                    Parties_TenantId = table.Column<Guid>(type: "uuid", nullable: true),
                    Policies_TenantId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "OrderItems",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Price = table.Column<decimal>(type: "numeric", nullable: false),
                    Quantity = table.Column<int>(type: "integer", nullable: false),
                    OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                    Seq = table.Column<long>(type: "bigint", nullable: false),
                    Products_ProductId = table.Column<Guid>(type: "uuid", nullable: true),
                    Products_ProductFeatureId = table.Column<Guid>(type: "uuid", nullable: true),
                    Products_CoverageTypeId = table.Column<Guid>(type: "uuid", nullable: true),
                    Products_CoverageLevelId = table.Column<Guid>(type: "uuid", nullable: true),
                    OrderedForId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_OrderItems_OrderItems_OrderedForId",
                        column: x => x.OrderedForId,
                        principalSchema: "orders",
                        principalTable: "OrderItems",
                        principalColumn: "Id");
                    table.ForeignKey(
                        name: "FK_OrderItems_Orders_OrderId",
                        column: x => x.OrderId,
                        principalSchema: "orders",
                        principalTable: "Orders",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SalesOrders",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SalesOrders", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SalesOrders_Orders_Id",
                        column: x => x.Id,
                        principalSchema: "orders",
                        principalTable: "Orders",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "OrderItemRoles",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Seq = table.Column<int>(type: "integer", nullable: false),
                    OrderItemId = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderRoleTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    Parties_PartyId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderItemRoles", x => x.Id);
                    table.ForeignKey(
                        name: "FK_OrderItemRoles_OrderItems_OrderItemId",
                        column: x => x.OrderItemId,
                        principalSchema: "orders",
                        principalTable: "OrderItems",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_OrderItemRoles_OrderRoleTypes_OrderRoleTypeId",
                        column: x => x.OrderRoleTypeId,
                        principalSchema: "orders",
                        principalTable: "OrderRoleTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PoliciesAgreementItems",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderItemId = table.Column<Guid>(type: "uuid", nullable: false),
                    Policies_AgreementItemId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PoliciesAgreementItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PoliciesAgreementItems_OrderItems_OrderItemId",
                        column: x => x.OrderItemId,
                        principalSchema: "orders",
                        principalTable: "OrderItems",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SalesOrderItems",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SalesOrderItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SalesOrderItems_OrderItems_Id",
                        column: x => x.Id,
                        principalSchema: "orders",
                        principalTable: "OrderItems",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "MtiOriginalSalesOrders",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    SaleDate = table.Column<DateOnly>(type: "date", nullable: false),
                    LoanNumber = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MtiOriginalSalesOrders", x => x.Id);
                    table.ForeignKey(
                        name: "FK_MtiOriginalSalesOrders_SalesOrders_Id",
                        column: x => x.Id,
                        principalSchema: "orders",
                        principalTable: "SalesOrders",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "MtiOriginalSalesOrderItems",
                schema: "orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    OriginalId = table.Column<long>(type: "bigint", nullable: true),
                    TransID = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    Status = table.Column<string>(type: "character varying(5)", maxLength: 5, nullable: false),
                    ProductName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    PolicyType = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    PolicyNumber = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    Campaign = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    PolicyPreviousNumber = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    PolicyEffectiveDate = table.Column<DateOnly>(type: "date", nullable: true),
                    PolicyExpiryDate = table.Column<DateOnly>(type: "date", nullable: true),
                    RefPolicyType = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    Remark = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true),
                    RefNoticeNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    RefDetailNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    StatusMessage = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    RefQuotation = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    Source = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    SystemId = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    CustomerInfoNo = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    Package = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    Workshop = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    PayPlan = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    CollateralNo = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    VehicleCode = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    VehicleBrand = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    VehicleModel = table.Column<string>(type: "character varying(600)", maxLength: 600, nullable: true),
                    VehicleManufactoringYear = table.Column<int>(type: "integer", nullable: true),
                    VehicleColor = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    VehicleRegisterNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehicleRegisterProvince = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehicleRegisterYear = table.Column<int>(type: "integer", nullable: true),
                    VehicleChassis = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    VehicleCc = table.Column<float>(type: "real", nullable: true),
                    VehicleSeat = table.Column<float>(type: "real", nullable: true),
                    VehicleWeight = table.Column<float>(type: "real", nullable: true),
                    VehicleTonnage = table.Column<float>(type: "real", nullable: true),
                    VehicleEngine = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehiclePassenger = table.Column<int>(type: "integer", nullable: true),
                    InsuredAssetId = table.Column<Guid>(type: "uuid", nullable: true),
                    SumInsure = table.Column<decimal>(type: "numeric", nullable: false),
                    Deduct = table.Column<decimal>(type: "numeric", nullable: false),
                    DamageLifePerPerson = table.Column<decimal>(type: "numeric", nullable: false),
                    DamageLifePerTime = table.Column<decimal>(type: "numeric", nullable: false),
                    DamageInsurePerTime = table.Column<decimal>(type: "numeric", nullable: false),
                    AccidentPerDriver = table.Column<decimal>(type: "numeric", nullable: false),
                    MedicalInsure = table.Column<decimal>(type: "numeric", nullable: false),
                    InsureDriver = table.Column<decimal>(type: "numeric", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MtiOriginalSalesOrderItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_MtiOriginalSalesOrderItems_InsuredAssets_InsuredAssetId",
                        column: x => x.InsuredAssetId,
                        principalSchema: "orders",
                        principalTable: "InsuredAssets",
                        principalColumn: "Id");
                    table.ForeignKey(
                        name: "FK_MtiOriginalSalesOrderItems_SalesOrderItems_Id",
                        column: x => x.Id,
                        principalSchema: "orders",
                        principalTable: "SalesOrderItems",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_MtiOriginalSalesOrderItems_InsuredAssetId",
                schema: "orders",
                table: "MtiOriginalSalesOrderItems",
                column: "InsuredAssetId");

            migrationBuilder.CreateIndex(
                name: "IX_MtiOriginalSalesOrderItems_OriginalId",
                schema: "orders",
                table: "MtiOriginalSalesOrderItems",
                column: "OriginalId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_MtiOriginalSalesOrderItems_Status",
                schema: "orders",
                table: "MtiOriginalSalesOrderItems",
                column: "Status");

            migrationBuilder.CreateIndex(
                name: "IX_MtiOriginalSalesOrders_SaleDate_LoanNumber",
                schema: "orders",
                table: "MtiOriginalSalesOrders",
                columns: new[] { "SaleDate", "LoanNumber" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemRoles_OrderItemId",
                schema: "orders",
                table: "OrderItemRoles",
                column: "OrderItemId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemRoles_OrderRoleTypeId",
                schema: "orders",
                table: "OrderItemRoles",
                column: "OrderRoleTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemRoles_Seq_OrderItemId_OrderRoleTypeId_Parties_Part~",
                schema: "orders",
                table: "OrderItemRoles",
                columns: new[] { "Seq", "OrderItemId", "OrderRoleTypeId", "Parties_PartyId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderItems_OrderedForId",
                schema: "orders",
                table: "OrderItems",
                column: "OrderedForId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItems_OrderId_Seq_Products_ProductId_Products_ProductF~",
                schema: "orders",
                table: "OrderItems",
                columns: new[] { "OrderId", "Seq", "Products_ProductId", "Products_ProductFeatureId", "Products_CoverageTypeId", "Products_CoverageLevelId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderRoleTypes_Code",
                schema: "orders",
                table: "OrderRoleTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Orders_OrderNumber",
                schema: "orders",
                table: "Orders",
                column: "OrderNumber",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PoliciesAgreementItems_OrderItemId",
                schema: "orders",
                table: "PoliciesAgreementItems",
                column: "OrderItemId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "MtiOriginalSalesOrderItems",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "MtiOriginalSalesOrders",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "OrderItemRoles",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "PoliciesAgreementItems",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "InsuredAssets",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "SalesOrderItems",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "SalesOrders",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "OrderRoleTypes",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "OrderItems",
                schema: "orders");

            migrationBuilder.DropTable(
                name: "Orders",
                schema: "orders");
        }
    }
}
