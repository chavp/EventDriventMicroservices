using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ardalis.GuardClauses;
using FluentResults;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Mti.ProductManagement.Domain.Products;
using Mti.ProductManagement.Messaging.Products.Commands;
using Mti.ProductManagement.Persistance;

namespace Mti.ProductManagement.Application.Products.SaveProductMtiOriginal
{
    public sealed class SaveProductMtiOriginalCommandHandler
        : ICommandHandler<SaveProductMtiOriginalCommand, Result<SaveProductsByOrderResponse>>
    {
        private readonly ILogger _logger;
        protected readonly IDbContextFactory<ProductsContext> _dbFactory = null;
        private readonly string? _tenantId = null;

        public SaveProductMtiOriginalCommandHandler(
            ILogger<SaveProductMtiOriginalCommandHandler> logger,
            IDbContextFactory<ProductsContext> dbFactory)
        {
            Guard.Against.Null(logger);
            Guard.Against.Null(dbFactory);

            _dbFactory = dbFactory;
            _logger = logger;
        }

        public async Task<Result<SaveProductsByOrderResponse>> Handle(SaveProductMtiOriginalCommand request, CancellationToken cancellationToken)
        {
            try
            {
                Guard.Against.Null(request);
                Guard.Against.Null(request.Request);
                Guard.Against.Null(request.Request.OrderItems);
                Guard.Against.Zero(request.Request.OrderItems.Count);
                foreach (var item in request.Request.OrderItems)
                {
                    var countPick = (item.Product == null
                        && item.Coverage == null
                        && item.ProductFeature == null)?0:1;

                    Guard.Against.Zero(countPick, "Required pick one Product or Coverage or ProductFeature");
                    if(item.Product != null)
                    {
                        Guard.Against.NullOrEmpty(item.Product.ProductCode);
                        Guard.Against.NullOrEmpty(item.Product.ProductName);
                    }
                    if (item.Coverage != null)
                    {
                        Guard.Against.NullOrEmpty(item.Coverage.CoverageTypeCode);
                        Guard.Against.NullOrEmpty(item.Coverage.CoverageLevelTypeCode);
                        Guard.Against.NullOrEmpty(item.Coverage.CoverageLevelBasisCode);
                    }
                    if (item.ProductFeature != null)
                    {
                        Guard.Against.NullOrEmpty(item.ProductFeature.ProductFeatureTypeCode);
                        Guard.Against.NullOrEmpty(item.ProductFeature.ProductFeatureCode);
                        Guard.Against.NullOrEmpty(item.ProductFeature.ProductFeatureName);
                    }
                }
            }
            catch (ArgumentException ex)
            {
                return Result.Fail(new Error($"Request cannot be null: {ex.Message}"));
            }

            try
            {
                var resp = new SaveProductsByOrderResponse(request.Request.OrderId)
                {
                    Orders_TenantId = request.Request.Orders_TenantId,
                    Products_TenantId = _tenantId,
                };

                var respItems = new List<SaveProductsByOrderItemResponse>();
                using (var db = await _dbFactory.CreateDbContextAsync(cancellationToken))
                {
                    // Check if product already exists
                    foreach (var item in request.Request.OrderItems)
                    {
                        var respItem = new SaveProductsByOrderItemResponse(item.OrderItemId);
                        respItems.Add(respItem);

                        // save products
                        if (item.Product != null)
                        {
                            respItem.Product = await saveProductAsync(db, item.Product, cancellationToken);
                        }
                        else if (item.Coverage != null)
                        {
                            respItem.Coverage = await saveCoverageAsync(db, item.Coverage, cancellationToken);
                        }
                        else if (item.ProductFeature != null)
                        {
                            respItem.ProductFeature = await saveProductFeatureAsync(db, item.ProductFeature, cancellationToken);
                        }
                    }

                    
                }
                resp.OrderItems = respItems;
                return Result.Ok(resp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while saving the product");
                // Rollback transaction in case of error
                return Result.Fail(new Error($"An error occurred while saving the product")
                    .CausedBy(ex));
            }

        }
    
        private async Task<ProductResponse?> saveProductAsync(ProductsContext db, 
            ProductRequest request, 
            CancellationToken cancellationToken)
        {
            var existingProduct = await db.Products
                                .SingleOrDefaultAsync(p => p.Code == request.ProductCode, cancellationToken);
            if (existingProduct == null)
            {
                try
                {
                    // Create new product
                    existingProduct = new Product(request.ProductCode)
                    {
                        Name = request.ProductName
                    };
                    db.Products.Add(existingProduct);

                    await db.SaveChangesAsync(cancellationToken);
                }
                catch (DbUpdateException dbEx)
                {
                    // Handle database update exceptions
                    _logger.LogError(dbEx, "Database update error while saving product");
                    existingProduct = await db.Products
                        .SingleAsync(p => p.Code == request.ProductCode, cancellationToken);
                }
            }

            return new ProductResponse(existingProduct.Id.Value);
        }

        private async Task<CoverageResponse?> saveCoverageAsync(ProductsContext db,
            CoverageRequest request,
            CancellationToken cancellationToken)
        {
            Guid? coverageTypeId = null;
            Guid? coverageLevelId = null;
            var coverageType = await db.CoverageTypes
                .SingleOrDefaultAsync(ct => ct.Code == request.CoverageTypeCode, cancellationToken);
            if(coverageType == null) return null;
            coverageTypeId = coverageType.Id;

            var coverageLevelBasis = await db.CoverageLevelBasises
                .SingleOrDefaultAsync(clb => clb.Code == request.CoverageLevelBasisCode, cancellationToken);
            if(coverageLevelBasis == null) return null;

            var coverageLevelType = await db.CoverageLevelTypes
                .SingleOrDefaultAsync(clb => clb.Code == request.CoverageLevelTypeCode, cancellationToken);
            if (coverageLevelBasis == null) return null;

            if (!request.Amount.HasValue
                && !request.Percentage.HasValue
                && !request.LimitFrom.HasValue
                && !request.LimitTo.HasValue) return null;

            if (request.Amount.HasValue)
            {
                if (coverageLevelType.Code == CoverageLevelType.CoverageAmount)
                {
                    coverageLevelId = await trySaveCoverageAmount(db,
                        coverageLevelBasis.Id.Value,
                        coverageLevelType.Id.Value,
                        request.Amount.Value,
                        cancellationToken);
                }
                else if (coverageLevelType.Code == CoverageLevelType.Deductibility)
                {
                    coverageLevelId = await trySaveDeductibility(db,
                        coverageLevelBasis.Id.Value,
                        coverageLevelType.Id.Value,
                        request.Amount.Value,
                        cancellationToken);
                }
                else
                    throw new ArgumentException($"Not inplement CoverageLevelType: {coverageLevelType.Code}");
            }
            else
                throw new ArgumentException($"Not inplement CoverageLevelType: {coverageLevelType.Code}");

            return new CoverageResponse(coverageTypeId.Value, coverageLevelId.Value);
        }

        private async Task<ProductFeatureResponse?> saveProductFeatureAsync(ProductsContext db,
            ProductFeatureRequest request,
            CancellationToken cancellationToken)
        {
            Guid? productFeatureId = null;
            Guid? productFeatureTypeId = null;
            var productFeatureType = await db.ProductFeatureTypes
                .SingleOrDefaultAsync(ct => ct.Code == request.ProductFeatureTypeCode, cancellationToken);
            if (productFeatureType == null) return null;

            productFeatureTypeId = productFeatureType.Id;

            var productFeature = await db.ProductFeatures
                .SingleOrDefaultAsync(x => x.Code == request.ProductFeatureCode, cancellationToken);
            if (productFeature == null)
            {
                if(productFeatureType.Code == ProductFeatureType.VehicleCode)
                {
                    productFeature = new VehicleCode(productFeatureTypeId.Value, request.ProductFeatureCode);
                }
                else if (productFeatureType.Code == ProductFeatureType.VehicleYear)
                {
                    productFeature = new VehicleYear(productFeatureTypeId.Value, request.ProductFeatureCode);
                }
                else if (productFeatureType.Code == ProductFeatureType.VehicleModel)
                {
                    productFeature = new VehicleModel(productFeatureTypeId.Value, request.ProductFeatureCode);
                }
                else if (productFeatureType.Code == ProductFeatureType.VehicleBrand)
                {
                    productFeature = new VehicleBrand(productFeatureTypeId.Value, request.ProductFeatureCode);
                }
                else
                    throw new ArgumentOutOfRangeException(nameof(productFeatureType));

                try
                {
                    productFeature.Name = request.ProductFeatureName;
                    db.Add(productFeature);
                    await db.SaveChangesAsync(cancellationToken);
                }
                catch(DbUpdateException ex)
                {
                    productFeature = await db.ProductFeatures
                        .SingleOrDefaultAsync(x => x.Code == request.ProductFeatureCode, cancellationToken);
                }

                productFeatureId = productFeature.Id;
            }

            return new ProductFeatureResponse(productFeatureId.Value);
        }

        private async Task<Guid?> trySaveCoverageAmount(ProductsContext db,
            Guid coverageLevelBasisId,
            Guid coverageLevelTypeId,
            decimal amount,
            CancellationToken cancellationToken)
        {
            Guid? coverageLevelId = null;
            var existingCoverageLevel = await db.CoverageLevels
                                    .OfType<CoverageAmount>()
                                    .SingleOrDefaultAsync(
                                        c => c.CoverageLevelBasisId == coverageLevelBasisId &&
                                             c.CoverageLevelTypeId == coverageLevelTypeId &&
                                             c.Amount == amount,
                                        cancellationToken);
            if (existingCoverageLevel == null)
            {
                try
                {
                    existingCoverageLevel = new CoverageAmount(coverageLevelTypeId, coverageLevelBasisId)
                    {
                        Amount = amount,
                    };
                    db.Add(existingCoverageLevel);
                    await db.SaveChangesAsync(cancellationToken);
                }
                catch (DbUpdateException ex)
                {
                    existingCoverageLevel = await db.CoverageLevels
                            .OfType<CoverageAmount>()
                            .SingleOrDefaultAsync(
                                c => c.CoverageLevelBasisId == coverageLevelBasisId &&
                                     c.CoverageLevelTypeId == coverageLevelTypeId &&
                                     c.Amount == amount,
                                cancellationToken);
                }
            }

            coverageLevelId = existingCoverageLevel.Id;
            return coverageLevelId;
        }

        private async Task<Guid?> trySaveDeductibility(ProductsContext db,
            Guid coverageLevelBasisId,
            Guid coverageLevelTypeId,
            decimal amount,
            CancellationToken cancellationToken)
        {
            Guid? coverageLevelId = null;
            var existingCoverageLevel = await db.CoverageLevels
                                    .OfType<Deductibility>()
                                    .SingleOrDefaultAsync(
                                        c => c.CoverageLevelBasisId == coverageLevelBasisId &&
                                             c.CoverageLevelTypeId == coverageLevelTypeId &&
                                             c.Amount == amount,
                                        cancellationToken);
            if (existingCoverageLevel == null)
            {
                try
                {
                    existingCoverageLevel = new Deductibility(coverageLevelTypeId, coverageLevelBasisId)
                    {
                        Amount = amount,
                    };
                    db.Add(existingCoverageLevel);
                    await db.SaveChangesAsync(cancellationToken);
                }
                catch (DbUpdateException ex)
                {
                    existingCoverageLevel = await db.CoverageLevels
                            .OfType<Deductibility>()
                            .SingleOrDefaultAsync(
                                c => c.CoverageLevelBasisId == coverageLevelBasisId &&
                                     c.CoverageLevelTypeId == coverageLevelTypeId &&
                                     c.Amount == amount,
                                cancellationToken);
                }
            }

            coverageLevelId = existingCoverageLevel.Id;
            return coverageLevelId;
        }
    }

     
    public interface ICommandHandler<in TCommand, TResponse> 
        where TCommand : ICommand<TResponse>
    {
        Task<TResponse> Handle(TCommand request, CancellationToken cancellationToken);
    }
}
