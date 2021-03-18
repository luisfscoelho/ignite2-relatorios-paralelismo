defmodule ReportsGenerator do
  alias ReportsGenerator.Parser

  def call(file_names) when not is_list(file_names) do
    {:error, "Please provides a list of strings"}
  end

  def call(file_names) do
    file_names
    |> Task.async_stream(&build/1)
    |> Enum.reduce(
      report_acc(),
      fn {:ok, result}, report -> sum_reports(report, result) end
    )
  end

  def build(file_name) do
    file_name
    |> Parser.parse_file()
    |> Enum.reduce(report_acc(), &sun_values/2)
  end

  def sum_reports(
    %{all_hours: all_hours1, hours_per_month: hours_per_month1, hours_per_year: hours_per_year1},
    %{all_hours: all_hours2, hours_per_month: hours_per_month2, hours_per_year: hours_per_year2}
  ) do
    all_hours = merge(all_hours1, all_hours2)
    hours_per_month = merge(hours_per_month1, hours_per_month2)
    hours_per_year = merge(hours_per_year1, hours_per_year2)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp sun_values(line, report) do
    all_hours = sum_all_hours(line, report.all_hours)
    hours_per_month = sum_hours_per_month(line, report.hours_per_month)
    hours_per_year = sum_hours_per_year(line, report.hours_per_year)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp sum_all_hours([freelancer, hour, _, _, _], all_hours) do
    acc_hour = Map.get(all_hours, freelancer, 0)
    Map.put(all_hours, freelancer, acc_hour + hour)
  end

  defp sum_hours_per_month([freelancer, hour, _, month, _], hours_per_month) do
    freelancer_month = Map.get(hours_per_month, freelancer, %{})
    acc_month = Map.get(freelancer_month, month, 0)
    new_month = Map.put(freelancer_month, month, acc_month + hour)
    Map.put(hours_per_month, freelancer, new_month)
  end

  defp sum_hours_per_year([freelancer, hour, _, _, year], hours_per_year) do
    freelancer_year = Map.get(hours_per_year, freelancer, %{})
    acc_year = Map.get(freelancer_year, year, 0)
    new_year = Map.put(freelancer_year, year, acc_year + hour)
    Map.put(hours_per_year, freelancer, new_year)
  end

  defp merge(left, right) when is_map(left) do
    Map.merge(left, right, fn _key, left_val, right_val ->
      merge(left_val, right_val) end)
  end

  defp merge(left, right), do: left + right

  defp build_report(all_hours, hours_per_month, hours_per_year) do
    %{all_hours: all_hours, hours_per_month: hours_per_month, hours_per_year: hours_per_year}
  end

  defp report_acc, do: %{all_hours: %{}, hours_per_month: %{}, hours_per_year: %{}}
end
