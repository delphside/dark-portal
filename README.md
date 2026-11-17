# Dark Portal

An analysis tool for the World of Warcraft auction house, in design: it is to
collect auction house data hourly and turn it into prices, volumes and
crafting margins. It is
[application 2](https://github.com/delphside/programme/blob/main/records/2.1-applications.md)
of the Delphside programme, and its design is in its main project,
[#5](https://github.com/delphside/dark-portal/issues/5).

| Path | What it holds |
| --- | --- |
| [`sizing/`](sizing/README.md) | the sizing test's scripts and its first run's report |
| [`app-profile.toml`](app-profile.toml) | the technical profile the programme's tooling reads |
| [`CLAUDE.md`](CLAUDE.md) | the rules Claude follows in this repository, beyond the programme's |
| [`programme/`](https://github.com/delphside/programme) | the programme's submodule: its generic rules, skills and tooling |
| [`.claude/`](.claude/) | links into the submodule's skills and agents |
| [`.github/`](.github/) | the pull request template and the workflows |

The application's records are in the programme repository, under
[`applications/dark-portal/`](https://github.com/delphside/programme/tree/main/applications/dark-portal).

After a clone, and after a pull that moves the submodule's pin, run:

```sh
git submodule update --init
```

[1.1](https://github.com/delphside/programme/blob/main/docs/1.1-programme-tooling.md#two-kinds-of-tooling)
says how an application uses the programme's tooling.
