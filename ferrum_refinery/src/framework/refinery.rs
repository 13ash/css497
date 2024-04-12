use crate::framework::errors::FerrumRefineryError;
use ferrum_deposit::deposit::ferrum_deposit_client::FerrumDepositClient;

pub struct Refinery {
    pub input_location: String,
    pub output_location: String,
    pub mappers: Vec<String>,
    pub reducers: Vec<String>,
}
pub struct RefineryBuilder {
    input_location: Option<String>,
    output_location: Option<String>,
    mappers: Option<Vec<String>>,
    reducers: Option<Vec<String>>,
}

impl RefineryBuilder {
    pub fn new() -> Self {
        RefineryBuilder {
            input_location: None,
            output_location: None,
            mappers: None,
            reducers: None,
        }
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
        let input_location = self.input_location.ok_or(FerrumRefineryError::ConfigError(
            "Input location is required".to_string(),
        ))?;
        let output_location = self
            .output_location
            .ok_or(FerrumRefineryError::ConfigError(
                "Output location is required".to_string(),
            ))?;
        let mappers = self.mappers.ok_or(FerrumRefineryError::ConfigError(
            "list of mappers is required".to_string(),
        ))?;
        let reducers = self.reducers.ok_or(FerrumRefineryError::ConfigError(
            "list of reducers is required".to_string(),
        ))?;

        Ok(Refinery {
            input_location,
            output_location,
            mappers,
            reducers,
        })
    }
}

impl Refinery {}
