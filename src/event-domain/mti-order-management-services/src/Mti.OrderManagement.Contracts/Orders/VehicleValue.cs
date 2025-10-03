using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Messaging;
using Mti.PartyManagement.Messaging.Parties;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record VehicleValue
        : VehicleMessage
    {
        public VehicleValue(Builder builder)
        {
            Code = builder.Code;
            Brand = builder.Brand;
            Model = builder.Model;
            ManufactoringYear = builder.ManufactoringYear;
            Color = builder.Color;
            RegisterNo = builder.RegisterNo;
            RegisterProvince = builder.RegisterProvince;
            RegisterYear = builder.RegisterYear;
            Chassis = builder.Chassis;
            Cc = builder.Cc;
            Seat = builder.Seat;
            Weight = builder.Weight;
            Tonnage = builder.Tonnage;
            Engine = builder.Engine;
            Passenger = builder.Passenger;
        }

        public static Builder CreateBuilder() => new();

        public sealed class Builder
        {
            public string? Code { get; private set; }
            public string? Brand { get; private set; }
            public string? Model { get; private set; }
            public ushort? ManufactoringYear { get; private set; }
            public string? Color { get; private set; }
            public string? RegisterNo { get; private set; }
            public string? RegisterProvince { get; private set; }
            public ushort? RegisterYear { get; private set; }
            public string? Chassis { get; private set; }
            public float? Cc { get; private set; }
            public float? Seat { get; private set; }
            public float? Weight { get; private set; }
            public float? Tonnage { get; private set; }
            public string? Engine { get; private set; }
            public ushort? Passenger { get; private set; }

            public Builder WithCode(string? code)
            {
                Code = code;
                return this;
            }
            public Builder WithBrand(string? brand)
            {
                Brand = brand;
                return this;
            }
            public Builder WithModel(string? model)
            {
                Model = model;
                return this;
            }
            public Builder WithManufactoringYear(ushort? manufactoringYear)
            {
                ManufactoringYear = manufactoringYear;
                return this;
            }
            public Builder WithColor(string? color)
            {
                Color = color;
                return this;
            }
            public Builder WithRegisterNo(string? registerNo)
            {
                RegisterNo = registerNo;
                return this;
            }
            public Builder WithRegisterProvince(string? registerProvince)
            {
                RegisterProvince = registerProvince;
                return this;
            }
            public Builder WithRegisterYear(ushort? registerYear)
            {
                RegisterYear = registerYear;
                return this;
            }
            public Builder WithChassis(string? chassis)
            {
                Chassis = chassis;
                return this;
            }
            public Builder WithCc(float? cc)
            {
                Cc = cc;
                return this;
            }
            public Builder WithSeat(float? seat)
            {
                Seat = seat;
                return this;
            }
            public Builder WithWeight(float? weight)
            {
                Weight = weight;
                return this;
            }
            public Builder WithTonnage(float? tonnage)
            {
                Tonnage = tonnage;
                return this;
            }
            public Builder WithEngine(string? engine)
            {
                Engine = engine;
                return this;
            }
            public Builder WithPassenger(ushort? passenger)
            {
                Passenger = passenger;
                return this;
            }

            public VehicleValue Build() => new VehicleValue(this);
        }
    }
}
