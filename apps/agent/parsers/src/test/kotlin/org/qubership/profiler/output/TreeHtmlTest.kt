package org.qubership.profiler.output

import org.junit.jupiter.api.Test
import com.netcracker.profiler.agent.ParameterInfo
import com.netcracker.profiler.configuration.ParameterInfoDto
import com.netcracker.profiler.dom.ClobValues
import com.netcracker.profiler.dom.ProfiledTree
import com.netcracker.profiler.dom.TagDictionary
import com.netcracker.profiler.io.HotspotTag
import com.netcracker.profiler.io.TreeToJson
import com.netcracker.profiler.output.CallTreeMediator
import com.netcracker.profiler.output.CallTreeParams
import com.netcracker.profiler.sax.values.ClobValue
import com.netcracker.profiler.servlet.layout.SimpleLayout
import java.io.ByteArrayOutputStream

class TreeHtmlTest {
    @Test
    fun executeBatchSql() {
        val treeToJson = TreeToJson("t", 1000)
        val idTestMethod: Int
        val idExecuteBatch: Int
        val idSql : Int
        val idBinds : Int

        val clSelect1 = ClobValue("rootFolder", "server1", 1, 100).apply {
            value =
                """
                insert into "product_instance" ("id", "parent_id", "root_id", "external_id", "type", "name", "state", "customer_id", "quote_id", "description", "eligibility_param_id", "extended_eligibility", "product_order_id", "last_modified", "start_date", "offering_id", "override_mode", "place_ref_id", "contracted_date", "quantity", "termination_date", "number_of_installments", "disconnection_reason", "disconnection_reason_description", "model_version", "characteristics", "extended_attributes", "source_quote_item_id", "related_party_ref", "agreement_ref", "account_ref", "business_group", "version", "effective_date", "idempotency_key", "suspended", "product_specification_ref", "previous_business_group_id", "applied_business_barring_id", "billing_name", "marketing_bundle_component_id", "expiration_date", "original_expiration_date", "sales_date", "prohibit_manual_removal", "applied_by_business_rule_id", "billing_sync_version", "product_relationship", "creation_date", "changed_by") values (cast($1 as uuid), cast($2 as uuid), cast($3 as uuid), $4, $5, $6, $7, $8, $9, $10, cast($11 as uuid), cast($12 as json), $13, cast($14 as timestamp(6) with time zone), cast($15 as timestamp(6) with time zone), $16, $17, $18, cast($19 as timestamp(6) with time zone), $20, cast($21 as timestamp(6) with time zone), $22, $23, $24, $25, cast($26 as jsonb), cast($27 as jsonb), cast($28 as uuid), cast($29 as jsonb), cast($30 as jsonb), cast($31 as jsonb), cast($32 as uuid), $33, cast($34 as timestamp(6) with time zone), $35, $36, cast($37 as jsonb), cast($38 as uuid), cast($39 as uuid), $40, $41, cast($42 as timestamp(6) with time zone), cast($43 as timestamp(6) with time zone), cast($44 as timestamp(6) with time zone), $45, $46, $47, cast($48 as jsonb), cast($49 as timestamp(6) with time zone), cast($50 as jsonb))
                """.trimIndent()
        }
        val clSelect2 = ClobValue("rootFolder", "server1", 1, 200).apply {
            value =
                """
                insert into "product_instance" ("id", "parent_id", "root_id", "external_id", "type", "name", "state", "customer_id", "quote_id", "description", "eligibility_param_id", "extended_eligibility", "product_order_id", "last_modified", "start_date", "offering_id", "override_mode", "place_ref_id", "contracted_date", "quantity", "termination_date", "number_of_installments", "disconnection_reason", "disconnection_reason_description", "model_version", "characteristics", "extended_attributes", "source_quote_item_id", "related_party_ref", "agreement_ref", "account_ref", "business_group", "version", "effective_date", "idempotency_key", "suspended", "product_specification_ref", "previous_business_group_id", "applied_business_barring_id", "billing_name", "marketing_bundle_component_id", "expiration_date", "original_expiration_date", "sales_date", "prohibit_manual_removal", "applied_by_business_rule_id", "billing_sync_version", "product_relationship", "creation_date", "changed_by") values (cast($1 as uuid), cast($2 as uuid), cast($3 as uuid), $4, $5, $6, $7, $8, $9, $10, cast($11 as uuid), cast($12 as json), $13, cast($14 as timestamp(6) with time zone), cast($15 as timestamp(6) with time zone), $16, $17, $18, cast($19 as timestamp(6) with time zone), $20, cast($21 as timestamp(6) with time zone), $22, $23, $24, $25, cast($26 as jsonb), cast($27 as jsonb), cast($28 as uuid), cast($29 as jsonb), cast($30 as jsonb), cast($31 as jsonb), cast($32 as uuid), $33, cast($34 as timestamp(6) with time zone), $35, $36, cast($37 as jsonb), cast($38 as uuid), cast($39 as uuid), $40, $41, cast($42 as timestamp(6) with time zone), cast($43 as timestamp(6) with time zone), cast($44 as timestamp(6) with time zone), $45, $46, $47, cast($48 as jsonb), cast($49 as timestamp(6) with time zone), cast($50 as jsonb)),(cast($51 as uuid), cast($52 as uuid), cast($53 as uuid), $54, $55, $56, $57, $58, $59, $60, cast($61 as uuid), cast($62 as json), $63, cast($64 as timestamp(6) with time zone), cast($65 as timestamp(6) with time zone), $66, $67, $68, cast($69 as timestamp(6) with time zone), $70, cast($71 as timestamp(6) with time zone), $72, $73, $74, $75, cast($76 as jsonb), cast($77 as jsonb), cast($78 as uuid), cast($79 as jsonb), cast($80 as jsonb), cast($81 as jsonb), cast($82 as uuid), $83, cast($84 as timestamp(6) with time zone), $85, $86, cast($87 as jsonb), cast($88 as uuid), cast($89 as uuid), $90, $91, cast($92 as timestamp(6) with time zone), cast($93 as timestamp(6) with time zone), cast($94 as timestamp(6) with time zone), $95, $96, $97, cast($98 as jsonb), cast($99 as timestamp(6) with time zone), cast($100 as jsonb)),(cast($101 as uuid), cast($102 as uuid), cast($103 as uuid), $104, $105, $106, $107, $108, $109, $110, cast($111 as uuid), cast($112 as json), $113, cast($114 as timestamp(6) with time zone), cast($115 as timestamp(6) with time zone), $116, $117, $118, cast($119 as timestamp(6) with time zone), $120, cast($121 as timestamp(6) with time zone), $122, $123, $124, $125, cast($126 as jsonb), cast($127 as jsonb), cast($128 as uuid), cast($129 as jsonb), cast($130 as jsonb), cast($131 as jsonb), cast($132 as uuid), $133, cast($134 as timestamp(6) with time zone), $135, $136, cast($137 as jsonb), cast($138 as uuid), cast($139 as uuid), $140, $141, cast($142 as timestamp(6) with time zone), cast($143 as timestamp(6) with time zone), cast($144 as timestamp(6) with time zone), $145, $146, $147, cast($148 as jsonb), cast($149 as timestamp(6) with time zone), cast($150 as jsonb)),(cast($151 as uuid), cast($152 as uuid), cast($153 as uuid), $154, $155, $156, $157, $158, $159, $160, cast($161 as uuid), cast($162 as json), $163, cast($164 as timestamp(6) with time zone), cast($165 as timestamp(6) with time zone), $166, $167, $168, cast($169 as timestamp(6) with time zone), $170, cast($171 as timestamp(6) with time zone), $172, $173, $174, $175, cast($176 as jsonb), cast($177 as jsonb), cast($178 as uuid), cast($179 as jsonb), cast($180 as jsonb), cast($181 as jsonb), cast($182 as uuid), $183, cast($184 as timestamp(6) with time zone), $185, $186, cast($187 as jsonb), cast($188 as uuid), cast($189 as uuid), $190, $191, cast($192 as timestamp(6) with time zone), cast($193 as timestamp(6) with time zone), cast($194 as timestamp(6) with time zone), $195, $196, $197, cast($198 as jsonb), cast($199 as timestamp(6) with time zone), cast($200 as jsonb))
                """.trimIndent()
        }
        val clSelect3 = ClobValue("rootFolder", "server2", 1, 300).apply {
            value =
                """
                insert into "price" ("id", "type", "value", "valid_from", "valid_to", "product_instance_id") values (cast($1 as uuid), $2, cast($3 as json), cast($4 as timestamp(6) with time zone), cast($5 as timestamp(6) with time zone), cast($6 as uuid)) on conflict ("id") do update set "value" = cast($7 as json), "valid_from" = cast($8 as timestamp(6) with time zone), "valid_to" = cast($9 as timestamp(6) with time zone)
                """.trimIndent()
        }
        val clSelect4 = ClobValue("rootFolder", "server2", 1, 400).apply {
            value =
                """
                update "items_history" set "active_to" = cast($1 as timestamp(6) with time zone) where ("items_history"."id" = cast($2 as uuid) and "items_history"."active_to" is null)
                """.trimIndent()
        }
        val clSelect5 = ClobValue("rootFolder", "server2", 1, 500).apply {
            value =
                """
                insert into "items_history" ("id", "version", "schema_version", "active_from", "active_to", "is_snapshot", "item", "state", "idempotency_key", "migrated") values (cast($1 as uuid), $2, $3, cast($4 as timestamp(6) with time zone), cast($5 as timestamp(6) with time zone), $6, cast($7 as jsonb), $8, $9, $10),(cast($11 as uuid), $12, $13, cast($14 as timestamp(6) with time zone), cast($15 as timestamp(6) with time zone), $16, cast($17 as jsonb), $18, $19, $20)
                """.trimIndent()
        }
        val clSelect6 = ClobValue("rootFolder", "server2", 1, 600).apply {
            value =
                """
                insert into "items_history" ("id", "version", "schema_version", "active_from", "active_to", "is_snapshot", "item", "state", "idempotency_key", "migrated") values (cast($1 as uuid), $2, $3, cast($4 as timestamp(6) with time zone), cast($5 as timestamp(6) with time zone), $6, cast($7 as jsonb), $8, $9, $10),(cast($11 as uuid), $12, $13, cast($14 as timestamp(6) with time zone), cast($15 as timestamp(6) with time zone), $16, cast($17 as jsonb), $18, $19, $20),(cast($21 as uuid), $22, $23, cast($24 as timestamp(6) with time zone), cast($25 as timestamp(6) with time zone), $26, cast($27 as jsonb), $28, $29, $30),(cast($31 as uuid), $32, $33, cast($34 as timestamp(6) with time zone), cast($35 as timestamp(6) with time zone), $36, cast($37 as jsonb), $38, $39, $40)
                """.trimIndent()
        }
        val clSelect7 = ClobValue("rootFolder", "server2", 1, 700).apply {
            value =
                """
                insert into "items_history" ("id", "version", "schema_version", "active_from", "active_to", "is_snapshot", "item", "state", "idempotency_key", "migrated") values (cast($1 as uuid), $2, $3, cast($4 as timestamp(6) with time zone), cast($5 as timestamp(6) with time zone), $6, cast($7 as jsonb), $8, $9, $10),(cast($11 as uuid), $12, $13, cast($14 as timestamp(6) with time zone), cast($15 as timestamp(6) with time zone), $16, cast($17 as jsonb), $18, $19, $20),(cast($21 as uuid), $22, $23, cast($24 as timestamp(6) with time zone), cast($25 as timestamp(6) with time zone), $26, cast($27 as jsonb), $28, $29, $30),(cast($31 as uuid), $32, $33, cast($34 as timestamp(6) with time zone), cast($35 as timestamp(6) with time zone), $36, cast($37 as jsonb), $38, $39, $40)
                """.trimIndent()
        }
        val clBind1 = ClobValue("rootFolder", "server2", 2, 100).apply {
            value =
                """
                <[('40090568-3da8-45e9-a528-e8e894901cd4'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'Product', '[FIX] Parental Control #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), 'cb57ebb2-497a-40b5-812d-4fd0a55b8f09', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), (NULL), (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"806faaf0-07b8-434e-9642-6597275c767d","name":"Parental Control","version":"0.1"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}']>
                """.trimIndent()
        }
        val clBind2 = ClobValue("rootFolder", "server2", 2, 200).apply {
            value =
                """
                <[('07fd549d-0a7b-4ffe-8afd-a48cec6bce8e'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'Product', '[FIX] IPv6 Option #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), '0429edb2-ee20-4014-9a33-8b289adb05bf', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), (NULL), (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"7de750ed-3ffe-4264-87f9-a9b538908d79","name":"IPv6","version":"0.1"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}', ('bf5e56db-5609-44aa-b069-4585c5477f4f'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'OTS', 'SpeedUp 50Mbps #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), 'ab3893a0-e89c-4cf7-bfb3-9ee36d301d0b', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), '[{"attributeId":"5900099e-d9c9-41a4-bdb5-8ef853461e7a","offeringCharId":"f5e232c9-4d8c-431c-b8b3-829934654b4a","productSpecCharId":"5900099e-d9c9-41a4-bdb5-8ef853461e7a","name":"Bonus Speed","value":["50"]}]', (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"f4cc2aec-eb17-4e76-9633-d9a43f57fd1c","name":"Speed Boost","version":"0.0"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}']>
                """.trimIndent()
        }
        val clBind3 = ClobValue("rootFolder", "server2", 2, 300).apply {
            value =
                """
                <[('95b0dc47-1fb3-4504-bcc5-de3e646474a2'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'OTS', '[FIX] Standard Installation #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), 'eb8bc59c-51e2-4cf0-87db-bb822e242e05', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), (NULL), (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"052086d5-8a9f-47f0-a17b-f59c26712e6d","name":"CPE Installation","version":"0.2"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}', ('a3abe957-d0b0-4fca-978e-fffcc386ba95'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'Product', 'D-Link DIR-300 #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), '9cf7f5c8-72fe-441b-aca7-b73fbcba5e95', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), (NULL), (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"7c70978e-3215-47b1-b03f-9e80c9e96824","name":"CPE Rent","version":"0.4"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}', ('030e5158-f5c7-40c2-a7f0-0e30ccb9d246'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'Product', '[FIX] Fixed IP #1', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), '7e0aa78a-7f3c-4a05-9306-765a635a18f7', 'net', (NULL), (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), (NULL), (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', (NULL), (NULL), (NULL), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"c5162853-3c05-4344-8349-0c426eeccfbf","name":"Fixed Ip","version":"0.0"}', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}', ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), ('638ad870-a685-4850-b42d-40ec8036a911'::uuid), (NULL), 'Product', 'Home Fixed Internet 100Mbps #3', 'ACTIVATION_IN_PROGRESS', '0193b00b-cadc-7f92-99e5-b6c1ad2ed569', 'ecedc6f3-bc9d-47f7-a901-b9b44e6b7077', (NULL), ('05c11b5f-44f3-4dc1-86f9-becc3d3ebdb5'::uuid), (NULL), 'a2bcf912-29a4-49c9-aa78-f04f582ba7e6', '2024-12-10 10:15:09.191497805+00:00', (NULL), 'a2bc5fc7-a99c-4032-b190-262b9d1a1c0c', 'net', 'ddb64d40-4979-45eb-9a15-7fcb6e21356b', (NULL), ('1'::int4), (NULL), (NULL), (NULL), (NULL), ('5'::int4), '[{"attributeId":"a98e0459-1f9c-40a8-b0df-9e968e8a541e","offeringCharId":"2a0022cd-c99e-4051-9254-b06b983b440a","productSpecCharId":"a98e0459-1f9c-40a8-b0df-9e968e8a541e","name":"Technology","value":["Fiber"]},{"attributeId":"8a1307c6-bf79-4b1c-bebb-893b41038fdf","offeringCharId":"661d2012-31b8-4766-97e5-b56042cd251f","productSpecCharId":"8a1307c6-bf79-4b1c-bebb-893b41038fdf","name":"Download Speed","value":[100]},{"attributeId":"4e499b7b-ec0c-45c9-bf94-d6efa2c22c7b","offeringCharId":"2f7356ac-0b6a-46ec-9ae6-593a3a8c1584","productSpecCharId":"4e499b7b-ec0c-45c9-bf94-d6efa2c22c7b","name":"Upload Speed","value":["500"]}]', (NULL), (NULL), '[{"role":"Customer","refId":"0193b00b-cadc-7f92-99e5-b6c1ad2ed569","referredType":"Customer"}]', '[{"refId":"64bc514f-249f-4ae7-82a5-b4e12ecdfdfd","name":"Agreement #1951585","agreementType":"CommercialAgreement"}]', '[{"name":"Postpaid","refType":"Postpaid","refId":"e3fa9905-68ef-4ce5-a00b-923364c4f69a"}]', ('dc8aa1ff-2143-40c0-b933-d6b65b55eb7c'::uuid), ('1'::int8), (NULL), (NULL), (NULL), '{"id":"77007843-73fc-49fb-b8ae-f0f14a9dbb1d","name":"Internet","version":"0.15"}', (NULL), (NULL), (NULL), 'f94cb9cd-d3de-4c58-9273-aeaed9890ef0', (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), '2024-12-10 10:15:09.191497805+00:00', '{"userId":"945c2ddb-c7b6-4eb3-931a-4fdfc71764b2"}']>
                """.trimIndent()
        }

        val tree = ProfiledTree(
            TagDictionary().apply {
                idTestMethod = resolve("void org.qubership.cloud.crm.installbase.crud.repository.JooqBatchQueryRepository.executeBatchQuery(Query, List)")
                idExecuteBatch = resolve("void org.postgresql.core.v3.QueryExecutorImpl.execute(Query[], ParameterList[], BatchResultHandler, int, int, int, boolean)")
                idSql = resolve("sql")
                info(ParameterInfoDto("sql"))
                idBinds = resolve("binds")
                info(
                    ParameterInfoDto("binds").apply {
                        big = true
                        deduplicate = false
                    }
                )
            },
            ClobValues().apply {
                add(clSelect1)
                add(clSelect2)
                add(clSelect3)
                add(clSelect4)
                add(clSelect5)
                add(clSelect6)
                add(clSelect7)
                add(clBind1)
                add(clBind2)
                add(clBind3)
            }
        ).apply {
            root.getOrCreateChild(idTestMethod).apply {
                totalTime = 270
                childTime = 268
                count = 6
                getOrCreateChild(idExecuteBatch).apply {
                    totalTime = 268
                    childTime = 0
                    count = 6
                    addTag(
                        HotspotTag(idSql).apply {
                            totalTime = 225
                            count = 1
                            addValue(clSelect1)
                            addValue(clSelect2)
                        }
                    )
                    addTag(
                        HotspotTag(idSql).apply {
                            totalTime = 16
                            count = 1
                            addValue(clSelect3)
                        }
                    )
                    addTag(
                        HotspotTag(idSql).apply {
                            totalTime = 13
                            count = 1
                            addValue(clSelect4)
                        }
                    )
                    addTag(
                        HotspotTag(idSql).apply {
                            totalTime = 10
                            count = 1
                            addValue(clSelect5)
                            addValue(clSelect6)
                            addValue(clSelect7)
                        }
                    )
                    addTag(
                        HotspotTag(idBinds).apply {
                            totalTime = 225
                            count = 1
                            addValue(clBind1)
                            addValue(clBind2)
                            addValue(clBind3)
                        }
                    )
                }
            }
        }

        val out = ByteArrayOutputStream()
        val layout = SimpleLayout(out)
        val ctMed = CallTreeMediator(
            layout,
            "treedata",
            0,
            CallTreeParams(),
            10000,
            false
        )
        ctMed.visitTree(tree)
        ctMed.visitEnd()
        println(out.toString(Charsets.UTF_8.name()))

//        val jsonFactory = JsonFactory()
//        val sw = StringWriter()
//        jsonFactory.createGenerator(sw).use { json ->
//            treeToJson.serialize(tree, json)
//        }
//        println(sw.toString())
    }
}
