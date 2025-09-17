// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error;
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::rpc::store::PutRequest;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt, ensure};
use std::fmt::{Debug, Formatter};

const TEST_KEY_VALUE: &str = "/__test";

pub struct EtcdChecker {
    endpoints: String,
    etcd_kv_backend: KvBackendRef,
}

impl Debug for EtcdChecker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EtcdChecker")
    }
}

impl EtcdChecker {
    pub async fn try_new<E, S>(endpoints: S) -> error::Result<Self>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let endpoints_str = endpoints
            .as_ref()
            .iter()
            .map(|e| e.as_ref().to_string())
            .join(",");
        let etcd_kv_backend = EtcdStore::with_endpoints(endpoints, usize::MAX).await?;
        Ok(Self {
            endpoints: endpoints_str,
            etcd_kv_backend,
        })
    }

    pub async fn check_put_get(&self) -> error::Result<()> {
        self.etcd_kv_backend
            .put(PutRequest {
                key: TEST_KEY_VALUE.as_bytes().to_vec(),
                value: TEST_KEY_VALUE.as_bytes().to_vec(),
                prev_kv: false,
            })
            .await
            .context(error::EtcdOperationSnafu {
                endpoints: &self.endpoints,
            })?;
        let value = self
            .etcd_kv_backend
            .get(TEST_KEY_VALUE.as_bytes())
            .await
            .context(error::EtcdOperationSnafu {
                endpoints: &self.endpoints,
            })?
            .context(error::EtcdValueMismatchSnafu {
                endpoints: &self.endpoints,
                expect: TEST_KEY_VALUE,
                actual: "None",
            })?;

        ensure!(
            value.value.as_slice() == TEST_KEY_VALUE.as_bytes(),
            error::EtcdValueMismatchSnafu {
                endpoints: &self.endpoints,
                expect: TEST_KEY_VALUE,
                actual: format!("{:?}", value.value.as_slice()),
            }
        );
        self.etcd_kv_backend
            .delete(TEST_KEY_VALUE.as_bytes(), false)
            .await
            .context(error::EtcdOperationSnafu {
                endpoints: &self.endpoints,
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::metasrv::EtcdChecker;

    #[tokio::test]
    async fn test_connect_to_etcd_failed() {
        let checker = EtcdChecker::try_new(&["127.0.0.1:2379"]).await.unwrap();
        let result = checker.etcd_kv_backend.get(b"abcd").await;
        assert!(result.is_err());
    }
}
