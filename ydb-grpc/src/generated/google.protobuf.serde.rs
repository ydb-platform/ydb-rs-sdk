impl serde::Serialize for Any {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.type_url.is_empty() {
            len += 1;
        }
        if !self.value.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.Any", len)?;
        if !self.type_url.is_empty() {
            struct_ser.serialize_field("typeUrl", &self.type_url)?;
        }
        if !self.value.is_empty() {
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Any {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "typeUrl",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TypeUrl,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "typeUrl" => Ok(GeneratedField::TypeUrl),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Any;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Any")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Any, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut type_url__ = None;
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TypeUrl => {
                            if type_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeUrl"));
                            }
                            type_url__ = Some(map.next_value()?);
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Any {
                    type_url: type_url__.unwrap_or_default(),
                    value: value__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Any", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if !self.field.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        if !self.nested_type.is_empty() {
            len += 1;
        }
        if !self.enum_type.is_empty() {
            len += 1;
        }
        if !self.extension_range.is_empty() {
            len += 1;
        }
        if !self.oneof_decl.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        if !self.reserved_range.is_empty() {
            len += 1;
        }
        if !self.reserved_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.DescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if !self.field.is_empty() {
            struct_ser.serialize_field("field", &self.field)?;
        }
        if !self.extension.is_empty() {
            struct_ser.serialize_field("extension", &self.extension)?;
        }
        if !self.nested_type.is_empty() {
            struct_ser.serialize_field("nestedType", &self.nested_type)?;
        }
        if !self.enum_type.is_empty() {
            struct_ser.serialize_field("enumType", &self.enum_type)?;
        }
        if !self.extension_range.is_empty() {
            struct_ser.serialize_field("extensionRange", &self.extension_range)?;
        }
        if !self.oneof_decl.is_empty() {
            struct_ser.serialize_field("oneofDecl", &self.oneof_decl)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        if !self.reserved_range.is_empty() {
            struct_ser.serialize_field("reservedRange", &self.reserved_range)?;
        }
        if !self.reserved_name.is_empty() {
            struct_ser.serialize_field("reservedName", &self.reserved_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "field",
            "extension",
            "nestedType",
            "enumType",
            "extensionRange",
            "oneofDecl",
            "options",
            "reservedRange",
            "reservedName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Field,
            Extension,
            NestedType,
            EnumType,
            ExtensionRange,
            OneofDecl,
            Options,
            ReservedRange,
            ReservedName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "field" => Ok(GeneratedField::Field),
                            "extension" => Ok(GeneratedField::Extension),
                            "nestedType" => Ok(GeneratedField::NestedType),
                            "enumType" => Ok(GeneratedField::EnumType),
                            "extensionRange" => Ok(GeneratedField::ExtensionRange),
                            "oneofDecl" => Ok(GeneratedField::OneofDecl),
                            "options" => Ok(GeneratedField::Options),
                            "reservedRange" => Ok(GeneratedField::ReservedRange),
                            "reservedName" => Ok(GeneratedField::ReservedName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.DescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut field__ = None;
                let mut extension__ = None;
                let mut nested_type__ = None;
                let mut enum_type__ = None;
                let mut extension_range__ = None;
                let mut oneof_decl__ = None;
                let mut options__ = None;
                let mut reserved_range__ = None;
                let mut reserved_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = Some(map.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = Some(map.next_value()?);
                        }
                        GeneratedField::NestedType => {
                            if nested_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nestedType"));
                            }
                            nested_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::EnumType => {
                            if enum_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enumType"));
                            }
                            enum_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::ExtensionRange => {
                            if extension_range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extensionRange"));
                            }
                            extension_range__ = Some(map.next_value()?);
                        }
                        GeneratedField::OneofDecl => {
                            if oneof_decl__.is_some() {
                                return Err(serde::de::Error::duplicate_field("oneofDecl"));
                            }
                            oneof_decl__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReservedRange => {
                            if reserved_range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reservedRange"));
                            }
                            reserved_range__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReservedName => {
                            if reserved_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reservedName"));
                            }
                            reserved_name__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DescriptorProto {
                    name: name__,
                    field: field__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                    nested_type: nested_type__.unwrap_or_default(),
                    enum_type: enum_type__.unwrap_or_default(),
                    extension_range: extension_range__.unwrap_or_default(),
                    oneof_decl: oneof_decl__.unwrap_or_default(),
                    options: options__,
                    reserved_range: reserved_range__.unwrap_or_default(),
                    reserved_name: reserved_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.DescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for descriptor_proto::ExtensionRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.DescriptorProto.ExtensionRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for descriptor_proto::ExtensionRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = descriptor_proto::ExtensionRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.DescriptorProto.ExtensionRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<descriptor_proto::ExtensionRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                let mut options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(descriptor_proto::ExtensionRange {
                    start: start__,
                    end: end__,
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.DescriptorProto.ExtensionRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for descriptor_proto::ReservedRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.DescriptorProto.ReservedRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for descriptor_proto::ReservedRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = descriptor_proto::ReservedRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.DescriptorProto.ReservedRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<descriptor_proto::ReservedRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(descriptor_proto::ReservedRange {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.DescriptorProto.ReservedRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Duration {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seconds != 0 {
            len += 1;
        }
        if self.nanos != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.Duration", len)?;
        if self.seconds != 0 {
            struct_ser.serialize_field("seconds", ToString::to_string(&self.seconds).as_str())?;
        }
        if self.nanos != 0 {
            struct_ser.serialize_field("nanos", &self.nanos)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Duration {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seconds",
            "nanos",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seconds,
            Nanos,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "seconds" => Ok(GeneratedField::Seconds),
                            "nanos" => Ok(GeneratedField::Nanos),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Duration")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Duration, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seconds__ = None;
                let mut nanos__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Seconds => {
                            if seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seconds"));
                            }
                            seconds__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Nanos => {
                            if nanos__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nanos"));
                            }
                            nanos__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Duration {
                    seconds: seconds__.unwrap_or_default(),
                    nanos: nanos__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Duration", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Empty {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("google.protobuf.Empty", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Empty {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Empty;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Empty")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Empty, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Empty {
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Empty", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnumDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if !self.value.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        if !self.reserved_range.is_empty() {
            len += 1;
        }
        if !self.reserved_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.EnumDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if !self.value.is_empty() {
            struct_ser.serialize_field("value", &self.value)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        if !self.reserved_range.is_empty() {
            struct_ser.serialize_field("reservedRange", &self.reserved_range)?;
        }
        if !self.reserved_name.is_empty() {
            struct_ser.serialize_field("reservedName", &self.reserved_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnumDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "value",
            "options",
            "reservedRange",
            "reservedName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Value,
            Options,
            ReservedRange,
            ReservedName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "value" => Ok(GeneratedField::Value),
                            "options" => Ok(GeneratedField::Options),
                            "reservedRange" => Ok(GeneratedField::ReservedRange),
                            "reservedName" => Ok(GeneratedField::ReservedName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnumDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.EnumDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnumDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut value__ = None;
                let mut options__ = None;
                let mut reserved_range__ = None;
                let mut reserved_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReservedRange => {
                            if reserved_range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reservedRange"));
                            }
                            reserved_range__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReservedName => {
                            if reserved_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reservedName"));
                            }
                            reserved_name__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(EnumDescriptorProto {
                    name: name__,
                    value: value__.unwrap_or_default(),
                    options: options__,
                    reserved_range: reserved_range__.unwrap_or_default(),
                    reserved_name: reserved_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.EnumDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for enum_descriptor_proto::EnumReservedRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.EnumDescriptorProto.EnumReservedRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for enum_descriptor_proto::EnumReservedRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = enum_descriptor_proto::EnumReservedRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.EnumDescriptorProto.EnumReservedRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<enum_descriptor_proto::EnumReservedRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(enum_descriptor_proto::EnumReservedRange {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.EnumDescriptorProto.EnumReservedRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnumOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.allow_alias.is_some() {
            len += 1;
        }
        if self.deprecated.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.EnumOptions", len)?;
        if let Some(v) = self.allow_alias.as_ref() {
            struct_ser.serialize_field("allowAlias", v)?;
        }
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnumOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "allowAlias",
            "deprecated",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AllowAlias,
            Deprecated,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "allowAlias" => Ok(GeneratedField::AllowAlias),
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnumOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.EnumOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnumOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut allow_alias__ = None;
                let mut deprecated__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AllowAlias => {
                            if allow_alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("allowAlias"));
                            }
                            allow_alias__ = Some(map.next_value()?);
                        }
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(EnumOptions {
                    allow_alias: allow_alias__,
                    deprecated: deprecated__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.EnumOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnumValueDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.number.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.EnumValueDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.number.as_ref() {
            struct_ser.serialize_field("number", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnumValueDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "number",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Number,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "number" => Ok(GeneratedField::Number),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnumValueDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.EnumValueDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnumValueDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut number__ = None;
                let mut options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Number => {
                            if number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("number"));
                            }
                            number__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(EnumValueDescriptorProto {
                    name: name__,
                    number: number__,
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.EnumValueDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EnumValueOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.deprecated.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.EnumValueOptions", len)?;
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EnumValueOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "deprecated",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Deprecated,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EnumValueOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.EnumValueOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EnumValueOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut deprecated__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(EnumValueOptions {
                    deprecated: deprecated__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.EnumValueOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExtensionRangeOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.ExtensionRangeOptions", len)?;
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExtensionRangeOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExtensionRangeOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.ExtensionRangeOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExtensionRangeOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExtensionRangeOptions {
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.ExtensionRangeOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FieldDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.number.is_some() {
            len += 1;
        }
        if self.label.is_some() {
            len += 1;
        }
        if self.r#type.is_some() {
            len += 1;
        }
        if self.type_name.is_some() {
            len += 1;
        }
        if self.extendee.is_some() {
            len += 1;
        }
        if self.default_value.is_some() {
            len += 1;
        }
        if self.oneof_index.is_some() {
            len += 1;
        }
        if self.json_name.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        if self.proto3_optional.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.FieldDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.number.as_ref() {
            struct_ser.serialize_field("number", v)?;
        }
        if let Some(v) = self.label.as_ref() {
            let v = field_descriptor_proto::Label::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("label", &v)?;
        }
        if let Some(v) = self.r#type.as_ref() {
            let v = field_descriptor_proto::Type::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.type_name.as_ref() {
            struct_ser.serialize_field("typeName", v)?;
        }
        if let Some(v) = self.extendee.as_ref() {
            struct_ser.serialize_field("extendee", v)?;
        }
        if let Some(v) = self.default_value.as_ref() {
            struct_ser.serialize_field("defaultValue", v)?;
        }
        if let Some(v) = self.oneof_index.as_ref() {
            struct_ser.serialize_field("oneofIndex", v)?;
        }
        if let Some(v) = self.json_name.as_ref() {
            struct_ser.serialize_field("jsonName", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        if let Some(v) = self.proto3_optional.as_ref() {
            struct_ser.serialize_field("proto3Optional", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FieldDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "number",
            "label",
            "type",
            "typeName",
            "extendee",
            "defaultValue",
            "oneofIndex",
            "jsonName",
            "options",
            "proto3Optional",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Number,
            Label,
            Type,
            TypeName,
            Extendee,
            DefaultValue,
            OneofIndex,
            JsonName,
            Options,
            Proto3Optional,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "number" => Ok(GeneratedField::Number),
                            "label" => Ok(GeneratedField::Label),
                            "type" => Ok(GeneratedField::Type),
                            "typeName" => Ok(GeneratedField::TypeName),
                            "extendee" => Ok(GeneratedField::Extendee),
                            "defaultValue" => Ok(GeneratedField::DefaultValue),
                            "oneofIndex" => Ok(GeneratedField::OneofIndex),
                            "jsonName" => Ok(GeneratedField::JsonName),
                            "options" => Ok(GeneratedField::Options),
                            "proto3Optional" => Ok(GeneratedField::Proto3Optional),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FieldDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.FieldDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FieldDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut number__ = None;
                let mut label__ = None;
                let mut r#type__ = None;
                let mut type_name__ = None;
                let mut extendee__ = None;
                let mut default_value__ = None;
                let mut oneof_index__ = None;
                let mut json_name__ = None;
                let mut options__ = None;
                let mut proto3_optional__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Number => {
                            if number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("number"));
                            }
                            number__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Label => {
                            if label__.is_some() {
                                return Err(serde::de::Error::duplicate_field("label"));
                            }
                            label__ = Some(map.next_value::<field_descriptor_proto::Label>()? as i32);
                        }
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map.next_value::<field_descriptor_proto::Type>()? as i32);
                        }
                        GeneratedField::TypeName => {
                            if type_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeName"));
                            }
                            type_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Extendee => {
                            if extendee__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extendee"));
                            }
                            extendee__ = Some(map.next_value()?);
                        }
                        GeneratedField::DefaultValue => {
                            if default_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("defaultValue"));
                            }
                            default_value__ = Some(map.next_value()?);
                        }
                        GeneratedField::OneofIndex => {
                            if oneof_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("oneofIndex"));
                            }
                            oneof_index__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::JsonName => {
                            if json_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("jsonName"));
                            }
                            json_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                        GeneratedField::Proto3Optional => {
                            if proto3_optional__.is_some() {
                                return Err(serde::de::Error::duplicate_field("proto3Optional"));
                            }
                            proto3_optional__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FieldDescriptorProto {
                    name: name__,
                    number: number__,
                    label: label__,
                    r#type: r#type__,
                    type_name: type_name__,
                    extendee: extendee__,
                    default_value: default_value__,
                    oneof_index: oneof_index__,
                    json_name: json_name__,
                    options: options__,
                    proto3_optional: proto3_optional__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.FieldDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for field_descriptor_proto::Label {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Optional => "LABEL_OPTIONAL",
            Self::Required => "LABEL_REQUIRED",
            Self::Repeated => "LABEL_REPEATED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for field_descriptor_proto::Label {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "LABEL_OPTIONAL",
            "LABEL_REQUIRED",
            "LABEL_REPEATED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = field_descriptor_proto::Label;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_descriptor_proto::Label::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_descriptor_proto::Label::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "LABEL_OPTIONAL" => Ok(field_descriptor_proto::Label::Optional),
                    "LABEL_REQUIRED" => Ok(field_descriptor_proto::Label::Required),
                    "LABEL_REPEATED" => Ok(field_descriptor_proto::Label::Repeated),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for field_descriptor_proto::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Double => "TYPE_DOUBLE",
            Self::Float => "TYPE_FLOAT",
            Self::Int64 => "TYPE_INT64",
            Self::Uint64 => "TYPE_UINT64",
            Self::Int32 => "TYPE_INT32",
            Self::Fixed64 => "TYPE_FIXED64",
            Self::Fixed32 => "TYPE_FIXED32",
            Self::Bool => "TYPE_BOOL",
            Self::String => "TYPE_STRING",
            Self::Group => "TYPE_GROUP",
            Self::Message => "TYPE_MESSAGE",
            Self::Bytes => "TYPE_BYTES",
            Self::Uint32 => "TYPE_UINT32",
            Self::Enum => "TYPE_ENUM",
            Self::Sfixed32 => "TYPE_SFIXED32",
            Self::Sfixed64 => "TYPE_SFIXED64",
            Self::Sint32 => "TYPE_SINT32",
            Self::Sint64 => "TYPE_SINT64",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for field_descriptor_proto::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "TYPE_DOUBLE",
            "TYPE_FLOAT",
            "TYPE_INT64",
            "TYPE_UINT64",
            "TYPE_INT32",
            "TYPE_FIXED64",
            "TYPE_FIXED32",
            "TYPE_BOOL",
            "TYPE_STRING",
            "TYPE_GROUP",
            "TYPE_MESSAGE",
            "TYPE_BYTES",
            "TYPE_UINT32",
            "TYPE_ENUM",
            "TYPE_SFIXED32",
            "TYPE_SFIXED64",
            "TYPE_SINT32",
            "TYPE_SINT64",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = field_descriptor_proto::Type;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_descriptor_proto::Type::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_descriptor_proto::Type::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "TYPE_DOUBLE" => Ok(field_descriptor_proto::Type::Double),
                    "TYPE_FLOAT" => Ok(field_descriptor_proto::Type::Float),
                    "TYPE_INT64" => Ok(field_descriptor_proto::Type::Int64),
                    "TYPE_UINT64" => Ok(field_descriptor_proto::Type::Uint64),
                    "TYPE_INT32" => Ok(field_descriptor_proto::Type::Int32),
                    "TYPE_FIXED64" => Ok(field_descriptor_proto::Type::Fixed64),
                    "TYPE_FIXED32" => Ok(field_descriptor_proto::Type::Fixed32),
                    "TYPE_BOOL" => Ok(field_descriptor_proto::Type::Bool),
                    "TYPE_STRING" => Ok(field_descriptor_proto::Type::String),
                    "TYPE_GROUP" => Ok(field_descriptor_proto::Type::Group),
                    "TYPE_MESSAGE" => Ok(field_descriptor_proto::Type::Message),
                    "TYPE_BYTES" => Ok(field_descriptor_proto::Type::Bytes),
                    "TYPE_UINT32" => Ok(field_descriptor_proto::Type::Uint32),
                    "TYPE_ENUM" => Ok(field_descriptor_proto::Type::Enum),
                    "TYPE_SFIXED32" => Ok(field_descriptor_proto::Type::Sfixed32),
                    "TYPE_SFIXED64" => Ok(field_descriptor_proto::Type::Sfixed64),
                    "TYPE_SINT32" => Ok(field_descriptor_proto::Type::Sint32),
                    "TYPE_SINT64" => Ok(field_descriptor_proto::Type::Sint64),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for FieldOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.ctype.is_some() {
            len += 1;
        }
        if self.packed.is_some() {
            len += 1;
        }
        if self.jstype.is_some() {
            len += 1;
        }
        if self.lazy.is_some() {
            len += 1;
        }
        if self.unverified_lazy.is_some() {
            len += 1;
        }
        if self.deprecated.is_some() {
            len += 1;
        }
        if self.weak.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.FieldOptions", len)?;
        if let Some(v) = self.ctype.as_ref() {
            let v = field_options::CType::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("ctype", &v)?;
        }
        if let Some(v) = self.packed.as_ref() {
            struct_ser.serialize_field("packed", v)?;
        }
        if let Some(v) = self.jstype.as_ref() {
            let v = field_options::JsType::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("jstype", &v)?;
        }
        if let Some(v) = self.lazy.as_ref() {
            struct_ser.serialize_field("lazy", v)?;
        }
        if let Some(v) = self.unverified_lazy.as_ref() {
            struct_ser.serialize_field("unverifiedLazy", v)?;
        }
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if let Some(v) = self.weak.as_ref() {
            struct_ser.serialize_field("weak", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FieldOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ctype",
            "packed",
            "jstype",
            "lazy",
            "unverifiedLazy",
            "deprecated",
            "weak",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ctype,
            Packed,
            Jstype,
            Lazy,
            UnverifiedLazy,
            Deprecated,
            Weak,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ctype" => Ok(GeneratedField::Ctype),
                            "packed" => Ok(GeneratedField::Packed),
                            "jstype" => Ok(GeneratedField::Jstype),
                            "lazy" => Ok(GeneratedField::Lazy),
                            "unverifiedLazy" => Ok(GeneratedField::UnverifiedLazy),
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "weak" => Ok(GeneratedField::Weak),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FieldOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.FieldOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FieldOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ctype__ = None;
                let mut packed__ = None;
                let mut jstype__ = None;
                let mut lazy__ = None;
                let mut unverified_lazy__ = None;
                let mut deprecated__ = None;
                let mut weak__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Ctype => {
                            if ctype__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ctype"));
                            }
                            ctype__ = Some(map.next_value::<field_options::CType>()? as i32);
                        }
                        GeneratedField::Packed => {
                            if packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packed"));
                            }
                            packed__ = Some(map.next_value()?);
                        }
                        GeneratedField::Jstype => {
                            if jstype__.is_some() {
                                return Err(serde::de::Error::duplicate_field("jstype"));
                            }
                            jstype__ = Some(map.next_value::<field_options::JsType>()? as i32);
                        }
                        GeneratedField::Lazy => {
                            if lazy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lazy"));
                            }
                            lazy__ = Some(map.next_value()?);
                        }
                        GeneratedField::UnverifiedLazy => {
                            if unverified_lazy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unverifiedLazy"));
                            }
                            unverified_lazy__ = Some(map.next_value()?);
                        }
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::Weak => {
                            if weak__.is_some() {
                                return Err(serde::de::Error::duplicate_field("weak"));
                            }
                            weak__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FieldOptions {
                    ctype: ctype__,
                    packed: packed__,
                    jstype: jstype__,
                    lazy: lazy__,
                    unverified_lazy: unverified_lazy__,
                    deprecated: deprecated__,
                    weak: weak__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.FieldOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for field_options::CType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::String => "STRING",
            Self::Cord => "CORD",
            Self::StringPiece => "STRING_PIECE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for field_options::CType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "STRING",
            "CORD",
            "STRING_PIECE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = field_options::CType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_options::CType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_options::CType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "STRING" => Ok(field_options::CType::String),
                    "CORD" => Ok(field_options::CType::Cord),
                    "STRING_PIECE" => Ok(field_options::CType::StringPiece),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for field_options::JsType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::JsNormal => "JS_NORMAL",
            Self::JsString => "JS_STRING",
            Self::JsNumber => "JS_NUMBER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for field_options::JsType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "JS_NORMAL",
            "JS_STRING",
            "JS_NUMBER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = field_options::JsType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_options::JsType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(field_options::JsType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "JS_NORMAL" => Ok(field_options::JsType::JsNormal),
                    "JS_STRING" => Ok(field_options::JsType::JsString),
                    "JS_NUMBER" => Ok(field_options::JsType::JsNumber),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for FileDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.package.is_some() {
            len += 1;
        }
        if !self.dependency.is_empty() {
            len += 1;
        }
        if !self.public_dependency.is_empty() {
            len += 1;
        }
        if !self.weak_dependency.is_empty() {
            len += 1;
        }
        if !self.message_type.is_empty() {
            len += 1;
        }
        if !self.enum_type.is_empty() {
            len += 1;
        }
        if !self.service.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        if self.source_code_info.is_some() {
            len += 1;
        }
        if self.syntax.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.FileDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.package.as_ref() {
            struct_ser.serialize_field("package", v)?;
        }
        if !self.dependency.is_empty() {
            struct_ser.serialize_field("dependency", &self.dependency)?;
        }
        if !self.public_dependency.is_empty() {
            struct_ser.serialize_field("publicDependency", &self.public_dependency)?;
        }
        if !self.weak_dependency.is_empty() {
            struct_ser.serialize_field("weakDependency", &self.weak_dependency)?;
        }
        if !self.message_type.is_empty() {
            struct_ser.serialize_field("messageType", &self.message_type)?;
        }
        if !self.enum_type.is_empty() {
            struct_ser.serialize_field("enumType", &self.enum_type)?;
        }
        if !self.service.is_empty() {
            struct_ser.serialize_field("service", &self.service)?;
        }
        if !self.extension.is_empty() {
            struct_ser.serialize_field("extension", &self.extension)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        if let Some(v) = self.source_code_info.as_ref() {
            struct_ser.serialize_field("sourceCodeInfo", v)?;
        }
        if let Some(v) = self.syntax.as_ref() {
            struct_ser.serialize_field("syntax", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "package",
            "dependency",
            "publicDependency",
            "weakDependency",
            "messageType",
            "enumType",
            "service",
            "extension",
            "options",
            "sourceCodeInfo",
            "syntax",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Package,
            Dependency,
            PublicDependency,
            WeakDependency,
            MessageType,
            EnumType,
            Service,
            Extension,
            Options,
            SourceCodeInfo,
            Syntax,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "package" => Ok(GeneratedField::Package),
                            "dependency" => Ok(GeneratedField::Dependency),
                            "publicDependency" => Ok(GeneratedField::PublicDependency),
                            "weakDependency" => Ok(GeneratedField::WeakDependency),
                            "messageType" => Ok(GeneratedField::MessageType),
                            "enumType" => Ok(GeneratedField::EnumType),
                            "service" => Ok(GeneratedField::Service),
                            "extension" => Ok(GeneratedField::Extension),
                            "options" => Ok(GeneratedField::Options),
                            "sourceCodeInfo" => Ok(GeneratedField::SourceCodeInfo),
                            "syntax" => Ok(GeneratedField::Syntax),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.FileDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut package__ = None;
                let mut dependency__ = None;
                let mut public_dependency__ = None;
                let mut weak_dependency__ = None;
                let mut message_type__ = None;
                let mut enum_type__ = None;
                let mut service__ = None;
                let mut extension__ = None;
                let mut options__ = None;
                let mut source_code_info__ = None;
                let mut syntax__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Package => {
                            if package__.is_some() {
                                return Err(serde::de::Error::duplicate_field("package"));
                            }
                            package__ = Some(map.next_value()?);
                        }
                        GeneratedField::Dependency => {
                            if dependency__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dependency"));
                            }
                            dependency__ = Some(map.next_value()?);
                        }
                        GeneratedField::PublicDependency => {
                            if public_dependency__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publicDependency"));
                            }
                            public_dependency__ = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::WeakDependency => {
                            if weak_dependency__.is_some() {
                                return Err(serde::de::Error::duplicate_field("weakDependency"));
                            }
                            weak_dependency__ = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::MessageType => {
                            if message_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("messageType"));
                            }
                            message_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::EnumType => {
                            if enum_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enumType"));
                            }
                            enum_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::Service => {
                            if service__.is_some() {
                                return Err(serde::de::Error::duplicate_field("service"));
                            }
                            service__ = Some(map.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                        GeneratedField::SourceCodeInfo => {
                            if source_code_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceCodeInfo"));
                            }
                            source_code_info__ = Some(map.next_value()?);
                        }
                        GeneratedField::Syntax => {
                            if syntax__.is_some() {
                                return Err(serde::de::Error::duplicate_field("syntax"));
                            }
                            syntax__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FileDescriptorProto {
                    name: name__,
                    package: package__,
                    dependency: dependency__.unwrap_or_default(),
                    public_dependency: public_dependency__.unwrap_or_default(),
                    weak_dependency: weak_dependency__.unwrap_or_default(),
                    message_type: message_type__.unwrap_or_default(),
                    enum_type: enum_type__.unwrap_or_default(),
                    service: service__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                    options: options__,
                    source_code_info: source_code_info__,
                    syntax: syntax__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.FileDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileDescriptorSet {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.file.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.FileDescriptorSet", len)?;
        if !self.file.is_empty() {
            struct_ser.serialize_field("file", &self.file)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileDescriptorSet {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "file",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            File,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "file" => Ok(GeneratedField::File),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileDescriptorSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.FileDescriptorSet")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileDescriptorSet, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut file__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::File => {
                            if file__.is_some() {
                                return Err(serde::de::Error::duplicate_field("file"));
                            }
                            file__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FileDescriptorSet {
                    file: file__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.FileDescriptorSet", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.java_package.is_some() {
            len += 1;
        }
        if self.java_outer_classname.is_some() {
            len += 1;
        }
        if self.java_multiple_files.is_some() {
            len += 1;
        }
        if self.java_generate_equals_and_hash.is_some() {
            len += 1;
        }
        if self.java_string_check_utf8.is_some() {
            len += 1;
        }
        if self.optimize_for.is_some() {
            len += 1;
        }
        if self.go_package.is_some() {
            len += 1;
        }
        if self.cc_generic_services.is_some() {
            len += 1;
        }
        if self.java_generic_services.is_some() {
            len += 1;
        }
        if self.py_generic_services.is_some() {
            len += 1;
        }
        if self.php_generic_services.is_some() {
            len += 1;
        }
        if self.deprecated.is_some() {
            len += 1;
        }
        if self.cc_enable_arenas.is_some() {
            len += 1;
        }
        if self.objc_class_prefix.is_some() {
            len += 1;
        }
        if self.csharp_namespace.is_some() {
            len += 1;
        }
        if self.swift_prefix.is_some() {
            len += 1;
        }
        if self.php_class_prefix.is_some() {
            len += 1;
        }
        if self.php_namespace.is_some() {
            len += 1;
        }
        if self.php_metadata_namespace.is_some() {
            len += 1;
        }
        if self.ruby_package.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.FileOptions", len)?;
        if let Some(v) = self.java_package.as_ref() {
            struct_ser.serialize_field("javaPackage", v)?;
        }
        if let Some(v) = self.java_outer_classname.as_ref() {
            struct_ser.serialize_field("javaOuterClassname", v)?;
        }
        if let Some(v) = self.java_multiple_files.as_ref() {
            struct_ser.serialize_field("javaMultipleFiles", v)?;
        }
        if let Some(v) = self.java_generate_equals_and_hash.as_ref() {
            struct_ser.serialize_field("javaGenerateEqualsAndHash", v)?;
        }
        if let Some(v) = self.java_string_check_utf8.as_ref() {
            struct_ser.serialize_field("javaStringCheckUtf8", v)?;
        }
        if let Some(v) = self.optimize_for.as_ref() {
            let v = file_options::OptimizeMode::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("optimizeFor", &v)?;
        }
        if let Some(v) = self.go_package.as_ref() {
            struct_ser.serialize_field("goPackage", v)?;
        }
        if let Some(v) = self.cc_generic_services.as_ref() {
            struct_ser.serialize_field("ccGenericServices", v)?;
        }
        if let Some(v) = self.java_generic_services.as_ref() {
            struct_ser.serialize_field("javaGenericServices", v)?;
        }
        if let Some(v) = self.py_generic_services.as_ref() {
            struct_ser.serialize_field("pyGenericServices", v)?;
        }
        if let Some(v) = self.php_generic_services.as_ref() {
            struct_ser.serialize_field("phpGenericServices", v)?;
        }
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if let Some(v) = self.cc_enable_arenas.as_ref() {
            struct_ser.serialize_field("ccEnableArenas", v)?;
        }
        if let Some(v) = self.objc_class_prefix.as_ref() {
            struct_ser.serialize_field("objcClassPrefix", v)?;
        }
        if let Some(v) = self.csharp_namespace.as_ref() {
            struct_ser.serialize_field("csharpNamespace", v)?;
        }
        if let Some(v) = self.swift_prefix.as_ref() {
            struct_ser.serialize_field("swiftPrefix", v)?;
        }
        if let Some(v) = self.php_class_prefix.as_ref() {
            struct_ser.serialize_field("phpClassPrefix", v)?;
        }
        if let Some(v) = self.php_namespace.as_ref() {
            struct_ser.serialize_field("phpNamespace", v)?;
        }
        if let Some(v) = self.php_metadata_namespace.as_ref() {
            struct_ser.serialize_field("phpMetadataNamespace", v)?;
        }
        if let Some(v) = self.ruby_package.as_ref() {
            struct_ser.serialize_field("rubyPackage", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "javaPackage",
            "javaOuterClassname",
            "javaMultipleFiles",
            "javaGenerateEqualsAndHash",
            "javaStringCheckUtf8",
            "optimizeFor",
            "goPackage",
            "ccGenericServices",
            "javaGenericServices",
            "pyGenericServices",
            "phpGenericServices",
            "deprecated",
            "ccEnableArenas",
            "objcClassPrefix",
            "csharpNamespace",
            "swiftPrefix",
            "phpClassPrefix",
            "phpNamespace",
            "phpMetadataNamespace",
            "rubyPackage",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JavaPackage,
            JavaOuterClassname,
            JavaMultipleFiles,
            JavaGenerateEqualsAndHash,
            JavaStringCheckUtf8,
            OptimizeFor,
            GoPackage,
            CcGenericServices,
            JavaGenericServices,
            PyGenericServices,
            PhpGenericServices,
            Deprecated,
            CcEnableArenas,
            ObjcClassPrefix,
            CsharpNamespace,
            SwiftPrefix,
            PhpClassPrefix,
            PhpNamespace,
            PhpMetadataNamespace,
            RubyPackage,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "javaPackage" => Ok(GeneratedField::JavaPackage),
                            "javaOuterClassname" => Ok(GeneratedField::JavaOuterClassname),
                            "javaMultipleFiles" => Ok(GeneratedField::JavaMultipleFiles),
                            "javaGenerateEqualsAndHash" => Ok(GeneratedField::JavaGenerateEqualsAndHash),
                            "javaStringCheckUtf8" => Ok(GeneratedField::JavaStringCheckUtf8),
                            "optimizeFor" => Ok(GeneratedField::OptimizeFor),
                            "goPackage" => Ok(GeneratedField::GoPackage),
                            "ccGenericServices" => Ok(GeneratedField::CcGenericServices),
                            "javaGenericServices" => Ok(GeneratedField::JavaGenericServices),
                            "pyGenericServices" => Ok(GeneratedField::PyGenericServices),
                            "phpGenericServices" => Ok(GeneratedField::PhpGenericServices),
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "ccEnableArenas" => Ok(GeneratedField::CcEnableArenas),
                            "objcClassPrefix" => Ok(GeneratedField::ObjcClassPrefix),
                            "csharpNamespace" => Ok(GeneratedField::CsharpNamespace),
                            "swiftPrefix" => Ok(GeneratedField::SwiftPrefix),
                            "phpClassPrefix" => Ok(GeneratedField::PhpClassPrefix),
                            "phpNamespace" => Ok(GeneratedField::PhpNamespace),
                            "phpMetadataNamespace" => Ok(GeneratedField::PhpMetadataNamespace),
                            "rubyPackage" => Ok(GeneratedField::RubyPackage),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.FileOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut java_package__ = None;
                let mut java_outer_classname__ = None;
                let mut java_multiple_files__ = None;
                let mut java_generate_equals_and_hash__ = None;
                let mut java_string_check_utf8__ = None;
                let mut optimize_for__ = None;
                let mut go_package__ = None;
                let mut cc_generic_services__ = None;
                let mut java_generic_services__ = None;
                let mut py_generic_services__ = None;
                let mut php_generic_services__ = None;
                let mut deprecated__ = None;
                let mut cc_enable_arenas__ = None;
                let mut objc_class_prefix__ = None;
                let mut csharp_namespace__ = None;
                let mut swift_prefix__ = None;
                let mut php_class_prefix__ = None;
                let mut php_namespace__ = None;
                let mut php_metadata_namespace__ = None;
                let mut ruby_package__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JavaPackage => {
                            if java_package__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaPackage"));
                            }
                            java_package__ = Some(map.next_value()?);
                        }
                        GeneratedField::JavaOuterClassname => {
                            if java_outer_classname__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaOuterClassname"));
                            }
                            java_outer_classname__ = Some(map.next_value()?);
                        }
                        GeneratedField::JavaMultipleFiles => {
                            if java_multiple_files__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaMultipleFiles"));
                            }
                            java_multiple_files__ = Some(map.next_value()?);
                        }
                        GeneratedField::JavaGenerateEqualsAndHash => {
                            if java_generate_equals_and_hash__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaGenerateEqualsAndHash"));
                            }
                            java_generate_equals_and_hash__ = Some(map.next_value()?);
                        }
                        GeneratedField::JavaStringCheckUtf8 => {
                            if java_string_check_utf8__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaStringCheckUtf8"));
                            }
                            java_string_check_utf8__ = Some(map.next_value()?);
                        }
                        GeneratedField::OptimizeFor => {
                            if optimize_for__.is_some() {
                                return Err(serde::de::Error::duplicate_field("optimizeFor"));
                            }
                            optimize_for__ = Some(map.next_value::<file_options::OptimizeMode>()? as i32);
                        }
                        GeneratedField::GoPackage => {
                            if go_package__.is_some() {
                                return Err(serde::de::Error::duplicate_field("goPackage"));
                            }
                            go_package__ = Some(map.next_value()?);
                        }
                        GeneratedField::CcGenericServices => {
                            if cc_generic_services__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ccGenericServices"));
                            }
                            cc_generic_services__ = Some(map.next_value()?);
                        }
                        GeneratedField::JavaGenericServices => {
                            if java_generic_services__.is_some() {
                                return Err(serde::de::Error::duplicate_field("javaGenericServices"));
                            }
                            java_generic_services__ = Some(map.next_value()?);
                        }
                        GeneratedField::PyGenericServices => {
                            if py_generic_services__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pyGenericServices"));
                            }
                            py_generic_services__ = Some(map.next_value()?);
                        }
                        GeneratedField::PhpGenericServices => {
                            if php_generic_services__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phpGenericServices"));
                            }
                            php_generic_services__ = Some(map.next_value()?);
                        }
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::CcEnableArenas => {
                            if cc_enable_arenas__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ccEnableArenas"));
                            }
                            cc_enable_arenas__ = Some(map.next_value()?);
                        }
                        GeneratedField::ObjcClassPrefix => {
                            if objc_class_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("objcClassPrefix"));
                            }
                            objc_class_prefix__ = Some(map.next_value()?);
                        }
                        GeneratedField::CsharpNamespace => {
                            if csharp_namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csharpNamespace"));
                            }
                            csharp_namespace__ = Some(map.next_value()?);
                        }
                        GeneratedField::SwiftPrefix => {
                            if swift_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("swiftPrefix"));
                            }
                            swift_prefix__ = Some(map.next_value()?);
                        }
                        GeneratedField::PhpClassPrefix => {
                            if php_class_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phpClassPrefix"));
                            }
                            php_class_prefix__ = Some(map.next_value()?);
                        }
                        GeneratedField::PhpNamespace => {
                            if php_namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phpNamespace"));
                            }
                            php_namespace__ = Some(map.next_value()?);
                        }
                        GeneratedField::PhpMetadataNamespace => {
                            if php_metadata_namespace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phpMetadataNamespace"));
                            }
                            php_metadata_namespace__ = Some(map.next_value()?);
                        }
                        GeneratedField::RubyPackage => {
                            if ruby_package__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rubyPackage"));
                            }
                            ruby_package__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FileOptions {
                    java_package: java_package__,
                    java_outer_classname: java_outer_classname__,
                    java_multiple_files: java_multiple_files__,
                    java_generate_equals_and_hash: java_generate_equals_and_hash__,
                    java_string_check_utf8: java_string_check_utf8__,
                    optimize_for: optimize_for__,
                    go_package: go_package__,
                    cc_generic_services: cc_generic_services__,
                    java_generic_services: java_generic_services__,
                    py_generic_services: py_generic_services__,
                    php_generic_services: php_generic_services__,
                    deprecated: deprecated__,
                    cc_enable_arenas: cc_enable_arenas__,
                    objc_class_prefix: objc_class_prefix__,
                    csharp_namespace: csharp_namespace__,
                    swift_prefix: swift_prefix__,
                    php_class_prefix: php_class_prefix__,
                    php_namespace: php_namespace__,
                    php_metadata_namespace: php_metadata_namespace__,
                    ruby_package: ruby_package__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.FileOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for file_options::OptimizeMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Speed => "SPEED",
            Self::CodeSize => "CODE_SIZE",
            Self::LiteRuntime => "LITE_RUNTIME",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for file_options::OptimizeMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SPEED",
            "CODE_SIZE",
            "LITE_RUNTIME",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = file_options::OptimizeMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(file_options::OptimizeMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(file_options::OptimizeMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SPEED" => Ok(file_options::OptimizeMode::Speed),
                    "CODE_SIZE" => Ok(file_options::OptimizeMode::CodeSize),
                    "LITE_RUNTIME" => Ok(file_options::OptimizeMode::LiteRuntime),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for GeneratedCodeInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.annotation.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.GeneratedCodeInfo", len)?;
        if !self.annotation.is_empty() {
            struct_ser.serialize_field("annotation", &self.annotation)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GeneratedCodeInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "annotation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Annotation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "annotation" => Ok(GeneratedField::Annotation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GeneratedCodeInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.GeneratedCodeInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GeneratedCodeInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut annotation__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Annotation => {
                            if annotation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("annotation"));
                            }
                            annotation__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GeneratedCodeInfo {
                    annotation: annotation__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.GeneratedCodeInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for generated_code_info::Annotation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if self.source_file.is_some() {
            len += 1;
        }
        if self.begin.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.GeneratedCodeInfo.Annotation", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if let Some(v) = self.source_file.as_ref() {
            struct_ser.serialize_field("sourceFile", v)?;
        }
        if let Some(v) = self.begin.as_ref() {
            struct_ser.serialize_field("begin", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for generated_code_info::Annotation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "sourceFile",
            "begin",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            SourceFile,
            Begin,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "sourceFile" => Ok(GeneratedField::SourceFile),
                            "begin" => Ok(GeneratedField::Begin),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = generated_code_info::Annotation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.GeneratedCodeInfo.Annotation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<generated_code_info::Annotation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut source_file__ = None;
                let mut begin__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::SourceFile => {
                            if source_file__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceFile"));
                            }
                            source_file__ = Some(map.next_value()?);
                        }
                        GeneratedField::Begin => {
                            if begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("begin"));
                            }
                            begin__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(generated_code_info::Annotation {
                    path: path__.unwrap_or_default(),
                    source_file: source_file__,
                    begin: begin__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.GeneratedCodeInfo.Annotation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.values.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.ListValue", len)?;
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "values",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Values,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "values" => Ok(GeneratedField::Values),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.ListValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut values__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ListValue {
                    values: values__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.ListValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MessageOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.message_set_wire_format.is_some() {
            len += 1;
        }
        if self.no_standard_descriptor_accessor.is_some() {
            len += 1;
        }
        if self.deprecated.is_some() {
            len += 1;
        }
        if self.map_entry.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.MessageOptions", len)?;
        if let Some(v) = self.message_set_wire_format.as_ref() {
            struct_ser.serialize_field("messageSetWireFormat", v)?;
        }
        if let Some(v) = self.no_standard_descriptor_accessor.as_ref() {
            struct_ser.serialize_field("noStandardDescriptorAccessor", v)?;
        }
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if let Some(v) = self.map_entry.as_ref() {
            struct_ser.serialize_field("mapEntry", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MessageOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "messageSetWireFormat",
            "noStandardDescriptorAccessor",
            "deprecated",
            "mapEntry",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MessageSetWireFormat,
            NoStandardDescriptorAccessor,
            Deprecated,
            MapEntry,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "messageSetWireFormat" => Ok(GeneratedField::MessageSetWireFormat),
                            "noStandardDescriptorAccessor" => Ok(GeneratedField::NoStandardDescriptorAccessor),
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "mapEntry" => Ok(GeneratedField::MapEntry),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MessageOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.MessageOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MessageOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut message_set_wire_format__ = None;
                let mut no_standard_descriptor_accessor__ = None;
                let mut deprecated__ = None;
                let mut map_entry__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::MessageSetWireFormat => {
                            if message_set_wire_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("messageSetWireFormat"));
                            }
                            message_set_wire_format__ = Some(map.next_value()?);
                        }
                        GeneratedField::NoStandardDescriptorAccessor => {
                            if no_standard_descriptor_accessor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("noStandardDescriptorAccessor"));
                            }
                            no_standard_descriptor_accessor__ = Some(map.next_value()?);
                        }
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::MapEntry => {
                            if map_entry__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mapEntry"));
                            }
                            map_entry__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MessageOptions {
                    message_set_wire_format: message_set_wire_format__,
                    no_standard_descriptor_accessor: no_standard_descriptor_accessor__,
                    deprecated: deprecated__,
                    map_entry: map_entry__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.MessageOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MethodDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.input_type.is_some() {
            len += 1;
        }
        if self.output_type.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        if self.client_streaming.is_some() {
            len += 1;
        }
        if self.server_streaming.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.MethodDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.input_type.as_ref() {
            struct_ser.serialize_field("inputType", v)?;
        }
        if let Some(v) = self.output_type.as_ref() {
            struct_ser.serialize_field("outputType", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        if let Some(v) = self.client_streaming.as_ref() {
            struct_ser.serialize_field("clientStreaming", v)?;
        }
        if let Some(v) = self.server_streaming.as_ref() {
            struct_ser.serialize_field("serverStreaming", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MethodDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "inputType",
            "outputType",
            "options",
            "clientStreaming",
            "serverStreaming",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            InputType,
            OutputType,
            Options,
            ClientStreaming,
            ServerStreaming,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "inputType" => Ok(GeneratedField::InputType),
                            "outputType" => Ok(GeneratedField::OutputType),
                            "options" => Ok(GeneratedField::Options),
                            "clientStreaming" => Ok(GeneratedField::ClientStreaming),
                            "serverStreaming" => Ok(GeneratedField::ServerStreaming),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MethodDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.MethodDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MethodDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut input_type__ = None;
                let mut output_type__ = None;
                let mut options__ = None;
                let mut client_streaming__ = None;
                let mut server_streaming__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::InputType => {
                            if input_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputType"));
                            }
                            input_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::OutputType => {
                            if output_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputType"));
                            }
                            output_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                        GeneratedField::ClientStreaming => {
                            if client_streaming__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientStreaming"));
                            }
                            client_streaming__ = Some(map.next_value()?);
                        }
                        GeneratedField::ServerStreaming => {
                            if server_streaming__.is_some() {
                                return Err(serde::de::Error::duplicate_field("serverStreaming"));
                            }
                            server_streaming__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MethodDescriptorProto {
                    name: name__,
                    input_type: input_type__,
                    output_type: output_type__,
                    options: options__,
                    client_streaming: client_streaming__,
                    server_streaming: server_streaming__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.MethodDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MethodOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.deprecated.is_some() {
            len += 1;
        }
        if self.idempotency_level.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.MethodOptions", len)?;
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if let Some(v) = self.idempotency_level.as_ref() {
            let v = method_options::IdempotencyLevel::from_i32(*v)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("idempotencyLevel", &v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MethodOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "deprecated",
            "idempotencyLevel",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Deprecated,
            IdempotencyLevel,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "idempotencyLevel" => Ok(GeneratedField::IdempotencyLevel),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MethodOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.MethodOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MethodOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut deprecated__ = None;
                let mut idempotency_level__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::IdempotencyLevel => {
                            if idempotency_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("idempotencyLevel"));
                            }
                            idempotency_level__ = Some(map.next_value::<method_options::IdempotencyLevel>()? as i32);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MethodOptions {
                    deprecated: deprecated__,
                    idempotency_level: idempotency_level__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.MethodOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for method_options::IdempotencyLevel {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::IdempotencyUnknown => "IDEMPOTENCY_UNKNOWN",
            Self::NoSideEffects => "NO_SIDE_EFFECTS",
            Self::Idempotent => "IDEMPOTENT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for method_options::IdempotencyLevel {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "IDEMPOTENCY_UNKNOWN",
            "NO_SIDE_EFFECTS",
            "IDEMPOTENT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = method_options::IdempotencyLevel;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(method_options::IdempotencyLevel::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(method_options::IdempotencyLevel::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "IDEMPOTENCY_UNKNOWN" => Ok(method_options::IdempotencyLevel::IdempotencyUnknown),
                    "NO_SIDE_EFFECTS" => Ok(method_options::IdempotencyLevel::NoSideEffects),
                    "IDEMPOTENT" => Ok(method_options::IdempotencyLevel::Idempotent),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NullValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::NullValue => "NULL_VALUE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NullValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NULL_VALUE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NullValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(NullValue::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(NullValue::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NULL_VALUE" => Ok(NullValue::NullValue),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for OneofDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.OneofDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OneofDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OneofDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.OneofDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OneofDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(OneofDescriptorProto {
                    name: name__,
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.OneofDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OneofOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.OneofOptions", len)?;
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OneofOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OneofOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.OneofOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OneofOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(OneofOptions {
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.OneofOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ServiceDescriptorProto {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if !self.method.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.ServiceDescriptorProto", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if !self.method.is_empty() {
            struct_ser.serialize_field("method", &self.method)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ServiceDescriptorProto {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "method",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Method,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "method" => Ok(GeneratedField::Method),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ServiceDescriptorProto;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.ServiceDescriptorProto")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ServiceDescriptorProto, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut method__ = None;
                let mut options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Method => {
                            if method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("method"));
                            }
                            method__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ServiceDescriptorProto {
                    name: name__,
                    method: method__.unwrap_or_default(),
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.ServiceDescriptorProto", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ServiceOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.deprecated.is_some() {
            len += 1;
        }
        if !self.uninterpreted_option.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.ServiceOptions", len)?;
        if let Some(v) = self.deprecated.as_ref() {
            struct_ser.serialize_field("deprecated", v)?;
        }
        if !self.uninterpreted_option.is_empty() {
            struct_ser.serialize_field("uninterpretedOption", &self.uninterpreted_option)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ServiceOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "deprecated",
            "uninterpretedOption",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Deprecated,
            UninterpretedOption,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "deprecated" => Ok(GeneratedField::Deprecated),
                            "uninterpretedOption" => Ok(GeneratedField::UninterpretedOption),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ServiceOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.ServiceOptions")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ServiceOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut deprecated__ = None;
                let mut uninterpreted_option__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Deprecated => {
                            if deprecated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecated"));
                            }
                            deprecated__ = Some(map.next_value()?);
                        }
                        GeneratedField::UninterpretedOption => {
                            if uninterpreted_option__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uninterpretedOption"));
                            }
                            uninterpreted_option__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ServiceOptions {
                    deprecated: deprecated__,
                    uninterpreted_option: uninterpreted_option__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.ServiceOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SourceCodeInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.location.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.SourceCodeInfo", len)?;
        if !self.location.is_empty() {
            struct_ser.serialize_field("location", &self.location)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SourceCodeInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "location",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Location,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "location" => Ok(GeneratedField::Location),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SourceCodeInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.SourceCodeInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SourceCodeInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut location__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Location => {
                            if location__.is_some() {
                                return Err(serde::de::Error::duplicate_field("location"));
                            }
                            location__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SourceCodeInfo {
                    location: location__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.SourceCodeInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for source_code_info::Location {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if !self.span.is_empty() {
            len += 1;
        }
        if self.leading_comments.is_some() {
            len += 1;
        }
        if self.trailing_comments.is_some() {
            len += 1;
        }
        if !self.leading_detached_comments.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.SourceCodeInfo.Location", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if !self.span.is_empty() {
            struct_ser.serialize_field("span", &self.span)?;
        }
        if let Some(v) = self.leading_comments.as_ref() {
            struct_ser.serialize_field("leadingComments", v)?;
        }
        if let Some(v) = self.trailing_comments.as_ref() {
            struct_ser.serialize_field("trailingComments", v)?;
        }
        if !self.leading_detached_comments.is_empty() {
            struct_ser.serialize_field("leadingDetachedComments", &self.leading_detached_comments)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for source_code_info::Location {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "span",
            "leadingComments",
            "trailingComments",
            "leadingDetachedComments",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            Span,
            LeadingComments,
            TrailingComments,
            LeadingDetachedComments,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "span" => Ok(GeneratedField::Span),
                            "leadingComments" => Ok(GeneratedField::LeadingComments),
                            "trailingComments" => Ok(GeneratedField::TrailingComments),
                            "leadingDetachedComments" => Ok(GeneratedField::LeadingDetachedComments),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = source_code_info::Location;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.SourceCodeInfo.Location")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<source_code_info::Location, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut span__ = None;
                let mut leading_comments__ = None;
                let mut trailing_comments__ = None;
                let mut leading_detached_comments__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Span => {
                            if span__.is_some() {
                                return Err(serde::de::Error::duplicate_field("span"));
                            }
                            span__ = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::LeadingComments => {
                            if leading_comments__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leadingComments"));
                            }
                            leading_comments__ = Some(map.next_value()?);
                        }
                        GeneratedField::TrailingComments => {
                            if trailing_comments__.is_some() {
                                return Err(serde::de::Error::duplicate_field("trailingComments"));
                            }
                            trailing_comments__ = Some(map.next_value()?);
                        }
                        GeneratedField::LeadingDetachedComments => {
                            if leading_detached_comments__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leadingDetachedComments"));
                            }
                            leading_detached_comments__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(source_code_info::Location {
                    path: path__.unwrap_or_default(),
                    span: span__.unwrap_or_default(),
                    leading_comments: leading_comments__,
                    trailing_comments: trailing_comments__,
                    leading_detached_comments: leading_detached_comments__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.SourceCodeInfo.Location", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Struct {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fields.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.Struct", len)?;
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Struct {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fields",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fields,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fields" => Ok(GeneratedField::Fields),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Struct")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Struct, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fields__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Fields => {
                            if fields__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(Struct {
                    fields: fields__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Struct", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Timestamp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seconds != 0 {
            len += 1;
        }
        if self.nanos != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.Timestamp", len)?;
        if self.seconds != 0 {
            struct_ser.serialize_field("seconds", ToString::to_string(&self.seconds).as_str())?;
        }
        if self.nanos != 0 {
            struct_ser.serialize_field("nanos", &self.nanos)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Timestamp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seconds",
            "nanos",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seconds,
            Nanos,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "seconds" => Ok(GeneratedField::Seconds),
                            "nanos" => Ok(GeneratedField::Nanos),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Timestamp")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Timestamp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seconds__ = None;
                let mut nanos__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Seconds => {
                            if seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seconds"));
                            }
                            seconds__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Nanos => {
                            if nanos__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nanos"));
                            }
                            nanos__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Timestamp {
                    seconds: seconds__.unwrap_or_default(),
                    nanos: nanos__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Timestamp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UninterpretedOption {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.identifier_value.is_some() {
            len += 1;
        }
        if self.positive_int_value.is_some() {
            len += 1;
        }
        if self.negative_int_value.is_some() {
            len += 1;
        }
        if self.double_value.is_some() {
            len += 1;
        }
        if self.string_value.is_some() {
            len += 1;
        }
        if self.aggregate_value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.UninterpretedOption", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.identifier_value.as_ref() {
            struct_ser.serialize_field("identifierValue", v)?;
        }
        if let Some(v) = self.positive_int_value.as_ref() {
            struct_ser.serialize_field("positiveIntValue", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.negative_int_value.as_ref() {
            struct_ser.serialize_field("negativeIntValue", ToString::to_string(&v).as_str())?;
        }
        if let Some(v) = self.double_value.as_ref() {
            struct_ser.serialize_field("doubleValue", v)?;
        }
        if let Some(v) = self.string_value.as_ref() {
            struct_ser.serialize_field("stringValue", pbjson::private::base64::encode(&v).as_str())?;
        }
        if let Some(v) = self.aggregate_value.as_ref() {
            struct_ser.serialize_field("aggregateValue", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UninterpretedOption {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "identifierValue",
            "positiveIntValue",
            "negativeIntValue",
            "doubleValue",
            "stringValue",
            "aggregateValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            IdentifierValue,
            PositiveIntValue,
            NegativeIntValue,
            DoubleValue,
            StringValue,
            AggregateValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "identifierValue" => Ok(GeneratedField::IdentifierValue),
                            "positiveIntValue" => Ok(GeneratedField::PositiveIntValue),
                            "negativeIntValue" => Ok(GeneratedField::NegativeIntValue),
                            "doubleValue" => Ok(GeneratedField::DoubleValue),
                            "stringValue" => Ok(GeneratedField::StringValue),
                            "aggregateValue" => Ok(GeneratedField::AggregateValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UninterpretedOption;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.UninterpretedOption")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UninterpretedOption, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut identifier_value__ = None;
                let mut positive_int_value__ = None;
                let mut negative_int_value__ = None;
                let mut double_value__ = None;
                let mut string_value__ = None;
                let mut aggregate_value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::IdentifierValue => {
                            if identifier_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("identifierValue"));
                            }
                            identifier_value__ = Some(map.next_value()?);
                        }
                        GeneratedField::PositiveIntValue => {
                            if positive_int_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("positiveIntValue"));
                            }
                            positive_int_value__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::NegativeIntValue => {
                            if negative_int_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negativeIntValue"));
                            }
                            negative_int_value__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::DoubleValue => {
                            if double_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doubleValue"));
                            }
                            double_value__ = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::StringValue => {
                            if string_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stringValue"));
                            }
                            string_value__ = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::AggregateValue => {
                            if aggregate_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateValue"));
                            }
                            aggregate_value__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UninterpretedOption {
                    name: name__.unwrap_or_default(),
                    identifier_value: identifier_value__,
                    positive_int_value: positive_int_value__,
                    negative_int_value: negative_int_value__,
                    double_value: double_value__,
                    string_value: string_value__,
                    aggregate_value: aggregate_value__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.UninterpretedOption", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for uninterpreted_option::NamePart {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 2;
        let mut struct_ser = serializer.serialize_struct("google.protobuf.UninterpretedOption.NamePart", len)?;
        struct_ser.serialize_field("namePart", &self.name_part)?;
        struct_ser.serialize_field("isExtension", &self.is_extension)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for uninterpreted_option::NamePart {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "namePart",
            "isExtension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NamePart,
            IsExtension,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "namePart" => Ok(GeneratedField::NamePart),
                            "isExtension" => Ok(GeneratedField::IsExtension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = uninterpreted_option::NamePart;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.UninterpretedOption.NamePart")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<uninterpreted_option::NamePart, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name_part__ = None;
                let mut is_extension__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NamePart => {
                            if name_part__.is_some() {
                                return Err(serde::de::Error::duplicate_field("namePart"));
                            }
                            name_part__ = Some(map.next_value()?);
                        }
                        GeneratedField::IsExtension => {
                            if is_extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isExtension"));
                            }
                            is_extension__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(uninterpreted_option::NamePart {
                    name_part: name_part__.ok_or_else(|| serde::de::Error::missing_field("namePart"))?,
                    is_extension: is_extension__.ok_or_else(|| serde::de::Error::missing_field("isExtension"))?,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.UninterpretedOption.NamePart", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("google.protobuf.Value", len)?;
        if let Some(v) = self.kind.as_ref() {
            match v {
                value::Kind::NullValue(v) => {
                    let v = NullValue::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("nullValue", &v)?;
                }
                value::Kind::NumberValue(v) => {
                    struct_ser.serialize_field("numberValue", v)?;
                }
                value::Kind::StringValue(v) => {
                    struct_ser.serialize_field("stringValue", v)?;
                }
                value::Kind::BoolValue(v) => {
                    struct_ser.serialize_field("boolValue", v)?;
                }
                value::Kind::StructValue(v) => {
                    struct_ser.serialize_field("structValue", v)?;
                }
                value::Kind::ListValue(v) => {
                    struct_ser.serialize_field("listValue", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nullValue",
            "numberValue",
            "stringValue",
            "boolValue",
            "structValue",
            "listValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NullValue,
            NumberValue,
            StringValue,
            BoolValue,
            StructValue,
            ListValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "nullValue" => Ok(GeneratedField::NullValue),
                            "numberValue" => Ok(GeneratedField::NumberValue),
                            "stringValue" => Ok(GeneratedField::StringValue),
                            "boolValue" => Ok(GeneratedField::BoolValue),
                            "structValue" => Ok(GeneratedField::StructValue),
                            "listValue" => Ok(GeneratedField::ListValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct google.protobuf.Value")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NullValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullValue"));
                            }
                            kind__ = Some(value::Kind::NullValue(map.next_value::<NullValue>()? as i32));
                        }
                        GeneratedField::NumberValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numberValue"));
                            }
                            kind__ = Some(value::Kind::NumberValue(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            ));
                        }
                        GeneratedField::StringValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stringValue"));
                            }
                            kind__ = Some(value::Kind::StringValue(map.next_value()?));
                        }
                        GeneratedField::BoolValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("boolValue"));
                            }
                            kind__ = Some(value::Kind::BoolValue(map.next_value()?));
                        }
                        GeneratedField::StructValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("structValue"));
                            }
                            kind__ = Some(value::Kind::StructValue(map.next_value()?));
                        }
                        GeneratedField::ListValue => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listValue"));
                            }
                            kind__ = Some(value::Kind::ListValue(map.next_value()?));
                        }
                    }
                }
                Ok(Value {
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("google.protobuf.Value", FIELDS, GeneratedVisitor)
    }
}