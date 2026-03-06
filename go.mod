module go-wind-admin

go 1.25.5

require (
	entgo.io/ent v0.14.5
	//github.com/alibaba/higress/v2 v2.1.11
	github.com/aws/aws-sdk-go-v2 v1.41.1
	github.com/aws/aws-sdk-go-v2/credentials v1.19.7
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.21.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.96.0
	github.com/getkin/kin-openapi v0.133.0
	github.com/go-chi/chi/v5 v5.2.2
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/go-sql-driver/mysql v1.9.3
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/google/gnostic v0.7.1
	github.com/google/uuid v1.6.0
	github.com/google/wire v0.7.0
	github.com/jackc/pgx/v5 v5.8.0
	github.com/jinzhu/inflection v1.0.0
	github.com/lib/pq v1.10.9
	github.com/menta2k/protoc-gen-redact/v3 v3.0.0-20251106150014-896cdd075ab1
	github.com/mileusna/useragent v1.3.5
	github.com/minio/minio-go/v7 v7.0.98
	github.com/oapi-codegen/runtime v1.1.2
	github.com/prometheus/client_golang v1.23.2
	github.com/redis/go-redis/v9 v9.17.2
	github.com/stretchr/testify v1.11.1
	github.com/tx7do/go-crud/api v0.0.7
	github.com/tx7do/go-crud/entgo v0.0.38
	github.com/tx7do/go-crud/viewer v0.0.5
	github.com/tx7do/go-utils v1.1.34
	github.com/tx7do/go-utils/copierutil v0.0.6
	github.com/tx7do/go-utils/geoip v1.1.8
	github.com/tx7do/go-utils/jwtutil v0.0.3
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-utils/password v0.0.1
	github.com/tx7do/kratos-authn v1.1.9
	github.com/tx7do/kratos-authz v1.1.7
	github.com/tx7do/kratos-authz/middleware v1.1.12
	github.com/tx7do/kratos-bootstrap/api v0.0.34
	github.com/tx7do/kratos-bootstrap/bootstrap v0.1.16
	github.com/tx7do/kratos-bootstrap/cache/redis v0.1.1
	github.com/tx7do/kratos-bootstrap/database/ent v0.1.3
	github.com/tx7do/kratos-bootstrap/oss/minio v0.1.1
	github.com/tx7do/kratos-bootstrap/rpc v0.1.1
	github.com/tx7do/kratos-bootstrap/transport/sse v0.0.2
	github.com/tx7do/kratos-swagger-ui v0.0.1
	github.com/tx7do/kratos-transport/transport/sse v1.2.25
	github.com/yuin/gopher-lua v1.1.1
	go.etcd.io/etcd/client/v3 v3.6.7
	go.opentelemetry.io/otel/trace v1.39.0
	google.golang.org/genproto/googleapis/api v0.0.0-20260120221211-b8f7ae30c516
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.11
	k8s.io/api v0.35.0
	k8s.io/apimachinery v0.35.0
	k8s.io/client-go v0.35.0
	sigs.k8s.io/kueue v0.16.1
)

require go.temporal.io/sdk v1.40.0

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.17 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-openapi/jsonreference v0.21.3 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.4 // indirect
	github.com/go-openapi/swag/conv v0.25.4 // indirect
	github.com/go-openapi/swag/fileutils v0.25.4 // indirect
	github.com/go-openapi/swag/jsonname v0.25.4 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.4 // indirect
	github.com/go-openapi/swag/loading v0.25.4 // indirect
	github.com/go-openapi/swag/mangling v0.25.4 // indirect
	github.com/go-openapi/swag/netutils v0.25.4 // indirect
	github.com/go-openapi/swag/stringutils v0.25.4 // indirect
	github.com/go-openapi/swag/typeutils v0.25.4 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.4 // indirect
	github.com/go-test/deep v1.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nexus-rpc/sdk-go v0.5.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240409071808-615f978279ca // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.1 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/woodsbury/decimal128 v1.3.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.etcd.io/etcd/api/v3 v3.6.7 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.7 // indirect
	go.temporal.io/api v1.62.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/oauth2 v0.33.0 // indirect
	golang.org/x/term v0.39.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kube-openapi v0.0.0-20251125145642-4e65d59e963e // indirect
	k8s.io/utils v0.0.0-20251002143259-bc988d571ff4 // indirect
	sigs.k8s.io/controller-runtime v0.22.4 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.1 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)

require (
	ariga.io/atlas v1.0.0 // indirect
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20251209175733-2a1774d88802.1 // indirect
	buf.build/go/protovalidate v1.1.0 // indirect
	cel.dev/expr v0.25.1 // indirect
	dario.cat/mergo v1.0.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/XSAM/otelsql v0.41.0 // indirect
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/bmatcuk/doublestar v1.3.4 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	//github.com/bytedance/sonic v1.10.0-rc3 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clipperhouse/displaywidth v0.6.2 // indirect
	github.com/clipperhouse/stringish v0.1.1 // indirect
	github.com/clipperhouse/uax29/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-kratos/aegis v0.2.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/inflect v0.21.5 // indirect
	github.com/go-openapi/jsonpointer v0.22.3 // indirect
	github.com/go-openapi/swag v0.25.4 // indirect
	github.com/go-playground/form/v4 v4.3.0 // indirect
	github.com/google/cel-go v0.26.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/subcommands v1.2.0 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/hashicorp/hcl/v2 v2.24.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/lithammer/shortuuid/v4 v4.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.19 // indirect
	github.com/mattn/go-sqlite3 v1.14.33 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037 // indirect
	github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90 // indirect
	github.com/olekukonko/cat v0.0.0-20250911104152-50322a0618f6 // indirect
	github.com/olekukonko/errors v1.1.0 // indirect
	github.com/olekukonko/ll v0.1.3 // indirect
	github.com/olekukonko/tablewriter v1.1.2 // indirect
	github.com/openzipkin/zipkin-go v0.4.3 // indirect
	github.com/oschwald/geoip2-golang v1.13.0 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.17.2 // indirect
	github.com/redis/go-redis/extra/redisotel/v9 v9.17.2 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/segmentio/ksuid v1.0.4 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shoenig/go-m1cpu v0.1.7 // indirect
	github.com/sony/sonyflake v1.3.0 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stoewer/go-strcase v1.3.1 // indirect
	github.com/swaggest/swgui v1.8.5 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/tx7do/go-crud/audit v0.0.2 // indirect
	github.com/tx7do/go-crud/pagination v0.0.11 // indirect
	github.com/tx7do/go-utils/id v0.0.2 // indirect
	github.com/tx7do/kratos-bootstrap/config v0.2.2 // indirect
	github.com/tx7do/kratos-bootstrap/logger v0.1.2 // indirect
	github.com/tx7do/kratos-bootstrap/registry v0.2.2 // indirect
	github.com/tx7do/kratos-bootstrap/tracer v0.1.3 // indirect
	github.com/tx7do/kratos-transport v1.1.18 // indirect
	github.com/vearutop/statigz v1.5.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zclconf/go-cty v1.17.0 // indirect
	github.com/zclconf/go-cty-yaml v1.2.0 // indirect
	go.einride.tech/aip v0.80.0 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/zipkin v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk v1.39.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.47.0
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	google.golang.org/genproto v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	gopkg.in/cenkalti/backoff.v1 v1.1.0 // indirect
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.44.3 // indirect
)
