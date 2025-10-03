using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Messaging;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record MtiOriginalCoverageValue
        : MtiOriginalCoverageMessage
    {
        public MtiOriginalCoverageValue(Builder builder)
        {
            CoverageTypeCode = builder.CoverageType;
            SumInsure = builder.SumInsure;
            Deduct = builder.Deduct;
            DamageLifePerPerson = builder.DamageLifePerPerson;
            DamageLifePerTime = builder.DamageLifePerTime;
            DamageInsurePerTime = builder.DamageInsurePerTime;
            AccidentPerDriver = builder.AccidentPerDriver;
            MedicalInsure = builder.MedicalInsure;
            InsureDriver = builder.InsureDriver;
        }

        public static Builder CreateBuilder(string coverageType) => new(coverageType);

        public sealed class Builder
        {
            internal string CoverageType { get; private set; }
            internal decimal SumInsure { get; private set; }
            internal decimal Deduct { get; private set; }
            internal decimal DamageLifePerPerson { get; private set; }
            internal decimal DamageLifePerTime { get; private set; }
            internal decimal DamageInsurePerTime { get; private set; }
            internal decimal AccidentPerDriver { get; private set; }
            internal decimal MedicalInsure { get; private set; }
            internal decimal InsureDriver { get; private set; }
            public Builder WithSumInsure(decimal sumInsure)
            {
                SumInsure = sumInsure;
                return this;
            }
            public Builder WithDeduct(decimal deduct)
            {
                Deduct = deduct;
                return this;
            }
            public Builder WithDamageLifePerPerson(decimal damageLifePerPerson)
            {
                DamageLifePerPerson = damageLifePerPerson;
                return this;
            }
            public Builder WithDamageLifePerTime(decimal damageLifePerTime)
            {
                DamageLifePerTime = damageLifePerTime;
                return this;
            }
            public Builder WithDamageInsurePerTime(decimal damageInsurePerTime)
            {
                DamageInsurePerTime = damageInsurePerTime;
                return this;
            }
            public Builder WithAccidentPerDriver(decimal accidentPerDriver)
            {
                AccidentPerDriver = accidentPerDriver;
                return this;
            }
            public Builder WithMedicalInsure(decimal medicalInsure)
            {
                MedicalInsure = medicalInsure;
                return this;
            }
            public Builder WithInsureDriver(decimal insureDriver)
            {
                InsureDriver = insureDriver;
                return this;
            }

            public Builder(string coverageType) 
            {
                CoverageType = coverageType;
            }

            public MtiOriginalCoverageValue Build() => new MtiOriginalCoverageValue(this);
        }
    }
}
