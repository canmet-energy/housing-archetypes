# NRCan Housing Archetypes for Energy Analysis
This repository contains a library of archetypes representing low-rise Canadian residential buildings. The archetypes are intended to support energy analysis, but may prove useful for other applications. 

### Contents

This repository contains the following directories:

1. `documentation` contains literature describing the the development of the archetypes and their use
2. `data` contains tables that summarize the archetypes, as well as xml-formatted source files that describe their detailed measurements and contents. 

### Data Sources

Natural Resources Canada developed this library by consolidating data from two sources:

- Audits of Canadian homes completed through the EnerGuide rating program (see: https://www.nrcan.gc.ca/energy-efficiency/homes/what-energy-efficient-home/welcome-my-energuide/16654 )
- The Survey of Household Energy Use (SHEU), administered by Statistics Canada (see: https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/sheu/2015/tables.cfm )

### Format

Summary tables are comma-separated-value (.csv) formatted; source files containing audit data from the EnerGuide program are xml-formatted according to the HOT2000 .h2k schema. 

### Acknowledgements

**Rasoul Asaee** (Natural Resources Canada) led development of this library. This work draws extensively upon methods first demonstrated by **Lucas Swan** (Dalhousie University, see: https://www.researchgate.net/publication/254220736_Hybrid_residential_end-use_energy_and_greenhouse_gas_emissions_model_-_development_and_verification_for_Canada) 

Funding for this work was provided by the Natural Resources Canada's Office of Energy Research and Development, under the Green Infrastructure program.

### Contacts

- Rasoul Asaee (rasoul.asaee@nrcan-rncan.gc.ca)
- Alex Ferguson (alex.ferguson@nrcan-rncan.gc.ca)