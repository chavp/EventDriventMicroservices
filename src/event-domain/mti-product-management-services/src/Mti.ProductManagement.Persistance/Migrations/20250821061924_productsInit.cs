using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.ProductManagement.Persistance.Migrations
{
    /// <inheritdoc />
    public partial class productsInit : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "products");

            migrationBuilder.CreateTable(
                name: "CoverageLevelBasises",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageLevelBasises", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "CoverageLevelTypes",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageLevelTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "CoverageTypes",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "ProductCatogories",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    ParentProductCategoryId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductCatogories", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ProductCatogories_ProductCatogories_ParentProductCategoryId",
                        column: x => x.ParentProductCategoryId,
                        principalSchema: "products",
                        principalTable: "ProductCatogories",
                        principalColumn: "Id");
                });

            migrationBuilder.CreateTable(
                name: "ProductFeatureTypes",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductFeatureTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Products",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Code = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: false),
                    Name = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Products", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "CoverageLevels",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CoverageLevelTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    CoverageLevelBasisId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageLevels", x => x.Id);
                    table.ForeignKey(
                        name: "FK_CoverageLevels_CoverageLevelBasises_CoverageLevelBasisId",
                        column: x => x.CoverageLevelBasisId,
                        principalSchema: "products",
                        principalTable: "CoverageLevelBasises",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_CoverageLevels_CoverageLevelTypes_CoverageLevelTypeId",
                        column: x => x.CoverageLevelTypeId,
                        principalSchema: "products",
                        principalTable: "CoverageLevelTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ProductFeatures",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Code = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    Name = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    ProductFeatureTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductFeatures", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ProductFeatures_ProductFeatureTypes_ProductFeatureTypeId",
                        column: x => x.ProductFeatureTypeId,
                        principalSchema: "products",
                        principalTable: "ProductFeatureTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ProductCatogoryClassifications",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductCatogoryId = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductCatogoryClassifications", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ProductCatogoryClassifications_ProductCatogories_ProductCat~",
                        column: x => x.ProductCatogoryId,
                        principalSchema: "products",
                        principalTable: "ProductCatogories",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ProductCatogoryClassifications_Products_ProductId",
                        column: x => x.ProductId,
                        principalSchema: "products",
                        principalTable: "Products",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "CoverageAmounts",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Amount = table.Column<decimal>(type: "numeric", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageAmounts", x => x.Id);
                    table.ForeignKey(
                        name: "FK_CoverageAmounts_CoverageLevels_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "CoverageLevels",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "CoverageAvailabilities",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductId = table.Column<Guid>(type: "uuid", nullable: false),
                    CoverageTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    CoverageLevelId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CoverageAvailabilities", x => x.Id);
                    table.ForeignKey(
                        name: "FK_CoverageAvailabilities_CoverageLevels_CoverageLevelId",
                        column: x => x.CoverageLevelId,
                        principalSchema: "products",
                        principalTable: "CoverageLevels",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_CoverageAvailabilities_CoverageTypes_CoverageTypeId",
                        column: x => x.CoverageTypeId,
                        principalSchema: "products",
                        principalTable: "CoverageTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_CoverageAvailabilities_Products_ProductId",
                        column: x => x.ProductId,
                        principalSchema: "products",
                        principalTable: "Products",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Deductibilities",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Amount = table.Column<decimal>(type: "numeric", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Deductibilities", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Deductibilities_CoverageLevels_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "CoverageLevels",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ProductFeatureAvailabilities",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductId = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductFeatureId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductFeatureAvailabilities", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ProductFeatureAvailabilities_ProductFeatures_ProductFeature~",
                        column: x => x.ProductFeatureId,
                        principalSchema: "products",
                        principalTable: "ProductFeatures",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ProductFeatureAvailabilities_Products_ProductId",
                        column: x => x.ProductId,
                        principalSchema: "products",
                        principalTable: "Products",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "VehicleBrands",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_VehicleBrands", x => x.Id);
                    table.ForeignKey(
                        name: "FK_VehicleBrands_ProductFeatures_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "ProductFeatures",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "VehicleCodes",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_VehicleCodes", x => x.Id);
                    table.ForeignKey(
                        name: "FK_VehicleCodes_ProductFeatures_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "ProductFeatures",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "VehicleModels",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_VehicleModels", x => x.Id);
                    table.ForeignKey(
                        name: "FK_VehicleModels_ProductFeatures_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "ProductFeatures",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "VehicleYears",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_VehicleYears", x => x.Id);
                    table.ForeignKey(
                        name: "FK_VehicleYears_ProductFeatures_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "ProductFeatures",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "OptionalCoverages",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OptionalCoverages", x => x.Id);
                    table.ForeignKey(
                        name: "FK_OptionalCoverages_CoverageAvailabilities_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "CoverageAvailabilities",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SelectableCoverages",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SelectableCoverages", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SelectableCoverages_CoverageAvailabilities_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "CoverageAvailabilities",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SelectableFeatures",
                schema: "products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SelectableFeatures", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SelectableFeatures_ProductFeatureAvailabilities_Id",
                        column: x => x.Id,
                        principalSchema: "products",
                        principalTable: "ProductFeatureAvailabilities",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_CoverageAvailabilities_CoverageLevelId",
                schema: "products",
                table: "CoverageAvailabilities",
                column: "CoverageLevelId");

            migrationBuilder.CreateIndex(
                name: "IX_CoverageAvailabilities_CoverageTypeId",
                schema: "products",
                table: "CoverageAvailabilities",
                column: "CoverageTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_CoverageAvailabilities_ProductId_CoverageTypeId_CoverageLev~",
                schema: "products",
                table: "CoverageAvailabilities",
                columns: new[] { "ProductId", "CoverageTypeId", "CoverageLevelId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_CoverageLevelBasises_Code",
                schema: "products",
                table: "CoverageLevelBasises",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_CoverageLevels_CoverageLevelBasisId",
                schema: "products",
                table: "CoverageLevels",
                column: "CoverageLevelBasisId");

            migrationBuilder.CreateIndex(
                name: "IX_CoverageLevels_CoverageLevelTypeId",
                schema: "products",
                table: "CoverageLevels",
                column: "CoverageLevelTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_CoverageLevelTypes_Code",
                schema: "products",
                table: "CoverageLevelTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_CoverageTypes_Code",
                schema: "products",
                table: "CoverageTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductCatogories_ParentProductCategoryId_Code",
                schema: "products",
                table: "ProductCatogories",
                columns: new[] { "ParentProductCategoryId", "Code" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductCatogoryClassifications_ProductCatogoryId_ProductId",
                schema: "products",
                table: "ProductCatogoryClassifications",
                columns: new[] { "ProductCatogoryId", "ProductId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductCatogoryClassifications_ProductId",
                schema: "products",
                table: "ProductCatogoryClassifications",
                column: "ProductId");

            migrationBuilder.CreateIndex(
                name: "IX_ProductFeatureAvailabilities_ProductFeatureId",
                schema: "products",
                table: "ProductFeatureAvailabilities",
                column: "ProductFeatureId");

            migrationBuilder.CreateIndex(
                name: "IX_ProductFeatureAvailabilities_ProductId_ProductFeatureId",
                schema: "products",
                table: "ProductFeatureAvailabilities",
                columns: new[] { "ProductId", "ProductFeatureId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductFeatures_Code_ProductFeatureTypeId",
                schema: "products",
                table: "ProductFeatures",
                columns: new[] { "Code", "ProductFeatureTypeId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductFeatures_ProductFeatureTypeId",
                schema: "products",
                table: "ProductFeatures",
                column: "ProductFeatureTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_ProductFeatureTypes_Code",
                schema: "products",
                table: "ProductFeatureTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Products_Code",
                schema: "products",
                table: "Products",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Products_Name",
                schema: "products",
                table: "Products",
                column: "Name");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "CoverageAmounts",
                schema: "products");

            migrationBuilder.DropTable(
                name: "Deductibilities",
                schema: "products");

            migrationBuilder.DropTable(
                name: "OptionalCoverages",
                schema: "products");

            migrationBuilder.DropTable(
                name: "ProductCatogoryClassifications",
                schema: "products");

            migrationBuilder.DropTable(
                name: "SelectableCoverages",
                schema: "products");

            migrationBuilder.DropTable(
                name: "SelectableFeatures",
                schema: "products");

            migrationBuilder.DropTable(
                name: "VehicleBrands",
                schema: "products");

            migrationBuilder.DropTable(
                name: "VehicleCodes",
                schema: "products");

            migrationBuilder.DropTable(
                name: "VehicleModels",
                schema: "products");

            migrationBuilder.DropTable(
                name: "VehicleYears",
                schema: "products");

            migrationBuilder.DropTable(
                name: "ProductCatogories",
                schema: "products");

            migrationBuilder.DropTable(
                name: "CoverageAvailabilities",
                schema: "products");

            migrationBuilder.DropTable(
                name: "ProductFeatureAvailabilities",
                schema: "products");

            migrationBuilder.DropTable(
                name: "CoverageLevels",
                schema: "products");

            migrationBuilder.DropTable(
                name: "CoverageTypes",
                schema: "products");

            migrationBuilder.DropTable(
                name: "ProductFeatures",
                schema: "products");

            migrationBuilder.DropTable(
                name: "Products",
                schema: "products");

            migrationBuilder.DropTable(
                name: "CoverageLevelBasises",
                schema: "products");

            migrationBuilder.DropTable(
                name: "CoverageLevelTypes",
                schema: "products");

            migrationBuilder.DropTable(
                name: "ProductFeatureTypes",
                schema: "products");
        }
    }
}
