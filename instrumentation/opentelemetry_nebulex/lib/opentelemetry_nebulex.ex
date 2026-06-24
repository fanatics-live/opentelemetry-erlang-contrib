defmodule OpentelemetryNebulex do
  @moduledoc """
  OpentelemetryNebulex uses `telemetry` handlers to create `OpenTelemetry` spans
  from Nebulex command events.
  """

  alias OpenTelemetry.SemConv.Incubating.DBAttributes
  alias OpentelemetryNebulex.NebulexAttributes

  @tracer_id __MODULE__
  @statement_key_limit 10

  @doc """
  Initializes and configures telemetry handlers for a given cache.

  Example:

      OpentelemetryNebulex.setup([:blog, :partitioned_cache])
  """
  def setup(event_prefix, opts \\ []) do
    :telemetry.attach(
      {__MODULE__, event_prefix, :command_start},
      event_prefix ++ [:command, :start],
      &__MODULE__.handle_command_start/4,
      opts
    )

    :telemetry.attach(
      {__MODULE__, event_prefix, :command_stop},
      event_prefix ++ [:command, :stop],
      &__MODULE__.handle_command_stop/4,
      opts
    )

    :telemetry.attach(
      {__MODULE__, event_prefix, :command_exception},
      event_prefix ++ [:command, :exception],
      &__MODULE__.handle_command_exception/4,
      opts
    )
  end

  @doc """
  Initializes and configures telemetry handlers for all caches.

  Use the `[:nebulex, :cache, :init]` event to automatically discover caches, and attach
  the handlers dynamically. It only works for caches that start after this function is called.

  Example:

      OpentelemetryNebulex.setup_all()
  """
  def setup_all(opts \\ []) do
    :telemetry.attach(
      __MODULE__,
      [:nebulex, :cache, :init],
      &__MODULE__.handle_init/4,
      opts
    )
  end

  @doc false
  def handle_init(_event, _measurements, metadata, config) do
    setup(metadata[:opts][:telemetry_prefix], config)
  end

  @doc false
  def handle_command_start(_event, _measurements, metadata, _config) do
    span_name = "nebulex #{db_operation(metadata)}"

    attributes =
      %{
        NebulexAttributes.nebulex_cache() => metadata.adapter_meta.cache,
        DBAttributes.db_system() => db_system(metadata),
        DBAttributes.db_operation_name() => db_operation(metadata),
        DBAttributes.db_query_text() => db_statement(metadata)
      }
      |> maybe_put(NebulexAttributes.nebulex_keyslot(), metadata.adapter_meta[:keyslot])
      |> maybe_put(NebulexAttributes.nebulex_model(), metadata.adapter_meta[:model])

    OpentelemetryTelemetry.start_telemetry_span(@tracer_id, span_name, metadata, %{
      attributes: attributes
    })
  end

  @doc false
  def handle_command_stop(_event, _measurements, metadata, _config) do
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, metadata)

    if action = extract_action(metadata) do
      OpenTelemetry.Span.set_attribute(ctx, NebulexAttributes.nebulex_action(), action)
    end

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, metadata)
  end

  @doc false
  def handle_command_exception(_event, _measurements, metadata, _config) do
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, metadata)

    OpenTelemetry.Span.record_exception(ctx, metadata.reason, metadata.stacktrace)
    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, format_error(metadata.reason)))

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, metadata)
  end

  defp maybe_put(attributes, _key, nil), do: attributes
  defp maybe_put(attributes, key, value), do: Map.put(attributes, key, value)

  defp extract_action(%{function_name: f, result: :"$expired"}) when f in [:get, :take], do: :miss
  defp extract_action(%{function_name: f, result: nil}) when f in [:get, :take], do: :miss
  defp extract_action(%{function_name: f, result: _}) when f in [:get, :take], do: :hit
  defp extract_action(_), do: nil

  defp format_error(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_error(error), do: inspect(error)

  defp db_system(%{adapter_meta: %{adapter: adapter}} = metadata) do
    db_system(adapter, metadata.adapter_meta)
  end

  defp db_system(%{adapter_meta: adapter_meta}) do
    adapter_meta[:backend]
  end

  defp db_system(adapter, _adapter_meta)
       when adapter in [Nebulex.Adapters.Redis, NebulexRedisAdapter],
       do: :redis

  defp db_system(_adapter, %{backend: backend}), do: backend
  defp db_system(_adapter, _adapter_meta), do: nil

  defp db_operation(%{function_name: operation}), do: operation
  defp db_operation(%{command: :fetch}), do: :get
  defp db_operation(%{command: :execute, args: [%{op: operation} | _tail]}), do: operation
  defp db_operation(%{command: operation}), do: operation
  defp db_operation(_), do: nil

  defp db_statement(%{args: args} = metadata) do
    case {db_operation(metadata), args} do
      {:get, [key | _tail]} ->
        "GET #{inspect(key)}"

      {:put, [key | _tail]} ->
        "PUT #{inspect(key)}"

      {:get_all, args} ->
        "MGET #{statement_keys(get_all_keys(args))}"

      {:put_all, args} ->
        "#{put_all_statement_command(args)} #{statement_keys(put_all_keys(args))}"

      _ ->
        nil
    end
  end

  defp db_statement(_metadata), do: nil

  defp get_all_keys([%{query: {:in, keys}} | _tail]), do: keys
  defp get_all_keys([[{:in, keys} | _tail] | _args]), do: keys
  defp get_all_keys([[keys] | _args]) when is_list(keys), do: keys
  defp get_all_keys([keys | _args]) when is_list(keys), do: keys
  defp get_all_keys(_args), do: []

  defp put_all_keys([entries | _args]) when is_map(entries) do
    Stream.map(entries, &entry_key/1)
  end

  defp put_all_keys([entries | _args]) when is_list(entries) do
    Stream.map(entries, &entry_key/1)
  end

  defp put_all_keys(_args), do: []

  defp entry_key({key, _value}), do: key
  defp entry_key(key), do: key

  defp put_all_statement_command([_entries, :put_new | _tail]), do: "MSETNX"
  defp put_all_statement_command(_args), do: "MSET"

  defp statement_keys(keys) do
    keys = Enum.take(keys, @statement_key_limit + 1)
    {keys, rest} = Enum.split(keys, @statement_key_limit)

    keys =
      keys
      |> Enum.map(&inspect/1)
      |> Enum.join(", ")

    suffix = if rest == [], do: "", else: ", ..."

    "[#{keys}#{suffix}]"
  end
end
