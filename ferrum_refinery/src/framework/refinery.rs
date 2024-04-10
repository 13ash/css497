use crate::api::map::Mapper;
use crate::api::reduce::Reducer;
use crate::framework::errors::FerrumRefineryError;
use ferrum_deposit::deposit::ferrum_deposit_client::FerrumDepositClient;

pub struct Refinery {
    pub input_location: String,
    pub output_location: String,
    pub mappers: Vec<String>,
    pub reducers: Vec<String>,
    pub deposit_client: FerrumDepositClient,
}
pub struct RefineryBuilder {
    deposit: Option<FerrumDepositClient>,
    input_location: Option<String>,
    output_location: Option<String>,
    mappers: Option<Vec<String>>,
    reducers: Option<Vec<String>>,
}

impl RefineryBuilder {
    pub fn new() -> Self {
        RefineryBuilder {
            deposit: None,
            input_location: None,
            output_location: None,
            mappers: None,
            reducers: None,
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

    pub fn with_mappers(mut self, mappers: Vec<String>) -> Self {
        self.mappers = Some(mappers);
        self
    }

    pub fn with_reducers(mut self, reducers: Vec<String>) -> Self {
        self.reducers = Some(reducers);
        self
    }

    pub fn build(self) -> Result<Refinery, FerrumRefineryError> {
        let deposit_client = self.deposit.ok_or(FerrumRefineryError::ConfigError(
            "Deposit Client required.".to_string(),
        ))?;
        let input_location = self.input_location.ok_or(FerrumRefineryError::ConfigError(
            "Input location is required".to_string(),
        ))?;
        let output_location = self
            .output_location
            .ok_or(FerrumRefineryError::ConfigError(
                "Output location is required".to_string(),
            ))?;
        let mappers = self.mappers.ok_or(FerrumRefineryError::ConfigError(
            "Number of mappers is required".to_string(),
        ))?;
        let reducers = self.reducers.ok_or(FerrumRefineryError::ConfigError(
            "Number of reducers is required".to_string(),
        ))?;

        Ok(Refinery {
            deposit_client,
            input_location,
            output_location,
            mappers,
            reducers,
        })
    }
}

impl Refinery {
    pub fn refine<
        ItemKey,
        ItemValue,
        IntermediateKey,
        IntermediateValue,
        FinalKey,
        FinalValue,
        M,
        R,
    >(
        &self,
        _mapper: M,
        _reducer: R,
    ) -> Result<(), FerrumRefineryError>
    where
        M: Mapper<ItemKey, ItemValue, IntermediateKey, IntermediateValue>,
        R: Reducer<IntermediateKey, IntermediateValue, FinalKey, FinalValue>,
    {
        // logic to start the map reduce process

        Ok(())
    }
}
