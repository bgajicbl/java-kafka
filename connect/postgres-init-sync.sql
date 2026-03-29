CREATE OR REPLACE FUNCTION public.generate_nanoid()
RETURNS text
LANGUAGE plpgsql
AS $function$
DECLARE
  nanoid text := '';
  characters text[] := array[
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-'
  ];
  i integer;
BEGIN
  FOR i IN 1..21 LOOP
    nanoid := nanoid || characters[(floor(random() * array_length(characters, 1)) + 1)];
  END LOOP;
  RETURN nanoid;
END;
$function$;

CREATE TABLE IF NOT EXISTS public.sync_runs (
  sync_run_id varchar(50) DEFAULT generate_nanoid() NOT NULL,
  started_at timestamp NULL,
  finished_at timestamp NULL,
  status varchar(20) NULL,
  created_at timestamp NULL,
  created_by varchar(50) NULL,
  updated_at timestamp NULL,
  updated_by varchar(50) NULL,
  sync_job_id varchar(50) NULL,
  job_info text NULL,
  CONSTRAINT sync_runs_pkey PRIMARY KEY (sync_run_id)
);

CREATE TABLE IF NOT EXISTS public.sync_run_payloads (
  sync_run_payload_id varchar(50) DEFAULT generate_nanoid() NOT NULL,
  sync_run_id varchar(50) NULL,
  transaction_id varchar(255) NULL,
  order_number varchar(50) NULL,
  order_amount float8 NULL,
  created_at timestamp NULL,
  updated_at timestamp NULL,
  venue_id varchar(50) NULL,
  customer_id varchar(50) NULL,
  line_items text NULL,
  order_date timestamp NULL,
  total_items int4 NULL,
  payments text NULL,
  note varchar(255) NULL,
  currency_code varchar(20) DEFAULT 'USD'::character varying NULL,
  currency_symbol varchar(20) DEFAULT '$'::character varying NULL,
  visit_alias varchar(50) NULL,
  CONSTRAINT sync_run_payloads_pkey PRIMARY KEY (sync_run_payload_id)
);

