use ferrum_deposit::deposit::ferrum_deposit_client::FerrumDepositClient;
use crate::api::map::Mapper;
use crate::api::reduce::Reducer;
use crate::framework::errors::RefineryError;

pub struct Refinery {
    pub input_location : String,
    pub output_location : String,
    pub num_mappers: usize,
    pub num_reducers: usize,
    pub deposit_client: FerrumDepositClient,
}
pub struct RefineryBuilder {
    deposit: Option<FerrumDepositClient>,
    input_location: Option<String>,
    output_location: Option<String>,
    num_mappers: Option<usize>,
    num_reducers: Option<usize>,
}

impl RefineryBuilder {
    pub fn new() -> Self {
        RefineryBuilder {
            deposit: None,
            input_location: None,
            output_location: None,
            num_mappers: None,
            num_reducers: None,
        }
    }

    pub fn with_deposit(mut self, deposit_client: FerrumDepositClient) -> Self {
        self.deposit = Some(deposit_client);
        self
    }

    pub fn with_input_location(mut self, input_location: String) -> Self {
        self.input_location = Some(input_location);
        self
    }

    pub fn with_output_location(mut self, output_location: String) -> Self {
        self.output_location = Some(output_location);
        self
    }

    pub fn with_num_mappers(mut self, num_mappers: usize) -> Self {
        self.num_mappers = Some(num_mappers);
        self
    }

    pub fn with_num_reducers(mut self, num_reducers: usize) -> Self {
        self.num_reducers = Some(num_reducers);
        self
    }

    pub fn build(self) -> Result<Refinery, RefineryError> {
        let deposit_client = self.deposit.ok_or(RefineryError::ConfigError("Deposit Client required.".to_string()))?;
        let input_location = self.input_location.ok_or(RefineryError::ConfigError("Input location is required".to_string()))?;
        let output_location = self.output_location.ok_or(RefineryError::ConfigError("Output location is required".to_string()))?;
        let num_mappers = self.num_mappers.ok_or(RefineryError::ConfigError("Number of mappers is required".to_string()))?;
        let num_reducers = self.num_reducers.ok_or(RefineryError::ConfigError("Number of reducers is required".to_string()))?;

        Ok(Refinery {
            deposit_client,
            input_location,
            output_location,
            num_mappers,
            num_reducers,
        })
    }
}

impl Refinery {
    pub fn refine<ItemKey, ItemValue, IntermediateKey, IntermediateValue, FinalKey, FinalValue, M, R>(
        &self,
        _mapper: M,
        _reducer: R,
    ) -> Result<(), RefineryError>
        where
            M: Mapper<ItemKey, ItemValue, IntermediateKey, IntermediateValue>,
            R: Reducer<IntermediateKey, IntermediateValue, FinalKey, FinalValue>,
    {
        Ok(())
    }
}
